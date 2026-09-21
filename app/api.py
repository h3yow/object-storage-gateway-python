"""HTTP layer: routes, identity headers, streaming bodies, stable envelopes."""
from __future__ import annotations

import logging
import uuid
from contextlib import asynccontextmanager
from typing import Any

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse, Response, StreamingResponse

from app import auth as auth_mod
from app.config import Settings, load_settings
from app.errors import ErrorCode, error, register_exception_handlers
from app.models import BucketPolicy
from app.store import GatewayStore

log = logging.getLogger("osg.api")


def _principal(request: Request) -> auth_mod.Principal:
    # Starlette headers are case-insensitive
    return auth_mod.principal_from_headers(
        {k.lower(): v for k, v in request.headers.items()}
    )


async def _chunked_body(request: Request):
    """Yield bounded raw chunks straight from the socket (no full buffering)."""
    async for chunk in request.stream():
        if chunk:
            yield chunk


def create_app(settings: Settings | None = None) -> FastAPI:
    settings = settings or load_settings()
    store = GatewayStore(settings)

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        logging.basicConfig(
            level=logging.INFO,
            format="%(asctime)s %(levelname)s %(name)s %(message)s")
        report = await store.start()
        log.info("startup recovery report: %s", report)
        yield

    app = FastAPI(
        title="Local Object Storage Gateway",
        version="0.1.0",
        docs_url="/docs",
        lifespan=lifespan,
    )
    app.state.settings = settings
    app.state.store = store
    register_exception_handlers(app)

    @app.middleware("http")
    async def request_context(request: Request, call_next):
        request.state.request_id = request.headers.get("x-request-id") \
            or uuid.uuid4().hex
        logging.getLogger("osg").debug(
            "request id=%s %s %s", request.state.request_id,
            request.method, request.url.path)
        return await call_next(request)

    @app.get("/")
    async def root() -> dict:
        return {"service": "object-storage-gateway", "status": "ok"}

    @app.get("/health")
    async def health() -> dict:
        return {"status": "ok"}

    # ---------- buckets ----------

    @app.get("/v1/buckets")
    async def list_buckets(request: Request) -> dict:
        principal = _principal(request)
        buckets = await store.list_buckets(principal)
        return {"buckets": buckets}

    @app.put("/v1/buckets/{bucket}")
    async def create_bucket(bucket: str, request: Request) -> JSONResponse:
        principal = _principal(request)
        body = await _optional_json(request)
        policy = BucketPolicy.model_validate(body["policy"]) \
            if body and body.get("policy") is not None else None
        quotas = (body or {}).get("quotas", {}) if body else {}
        await store.create_bucket(
            bucket, principal, policy,
            max_bytes=int(quotas.get("max_bytes", 0) or 0),
            max_objects=int(quotas.get("max_objects", 0) or 0),
        )
        return JSONResponse(
            status_code=201,
            content={"bucket": bucket, "owner": principal.name},
        )

    @app.delete("/v1/buckets/{bucket}")
    async def delete_bucket(bucket: str, request: Request) -> dict:
        principal = _principal(request)
        await store.delete_bucket(bucket, principal)
        return {"bucket": bucket, "deleted": True}

    @app.get("/v1/buckets/{bucket}/policy")
    async def get_policy(bucket: str, request: Request) -> dict:
        principal = _principal(request)
        policy = await store.get_bucket_policy(bucket, principal)
        return policy.model_dump()

    @app.put("/v1/buckets/{bucket}/policy")
    async def put_policy(bucket: str, request: Request) -> dict:
        principal = _principal(request)
        body = await _optional_json(request)
        if not body:
            raise error(ErrorCode.INVALID_ARGUMENT, "policy body required")
        policy = BucketPolicy.model_validate(body)
        await store.put_bucket_policy(bucket, principal, policy)
        return {"bucket": bucket, "policy": policy.model_dump()}

    # ---------- objects ----------

    @app.get("/v1/buckets/{bucket}/objects")
    async def list_objects(bucket: str, request: Request) -> dict:
        principal = _principal(request)
        q = request.query_params
        result = await store.list_objects(
            bucket, principal,
            prefix=q.get("prefix", ""),
            delimiter=q.get("delimiter"),
            marker=q.get("marker"),
            max_keys=int(q.get("max-keys", 0) or 0),
        )
        return result.model_dump()

    @app.put("/v1/buckets/{bucket}/objects/{key:path}/parts/{part_number:int}")
    async def upload_part(bucket: str, key: str, part_number: int,
                          request: Request) -> JSONResponse:
        principal = _principal(request)
        upload_id = request.query_params.get("uploadId")
        if not upload_id:
            raise error(ErrorCode.INVALID_ARGUMENT,
                        "uploadId query parameter required")
        n, etag = await store.upload_part(
            bucket, key, upload_id, part_number, principal,
            _chunked_body(request))
        return JSONResponse(
            status_code=200,
            content={"part_number": n, "etag": etag},
            headers={"ETag": f'"{etag}"'},
        )

    @app.put("/v1/buckets/{bucket}/objects/{key:path}")
    async def put_object(bucket: str, key: str, request: Request) -> JSONResponse:
        principal = _principal(request)
        meta = await store.put_object(
            bucket, key, principal,
            _chunked_body(request),
            if_match=request.headers.get("if-match"),
            if_none_match=request.headers.get("if-none-match"),
            content_type=request.headers.get("content-type"),
        )
        return JSONResponse(status_code=200, content=_object_headers(meta),
                            headers=_etag_headers(meta))

    @app.post("/v1/buckets/{bucket}/objects/{key:path}")
    async def post_object_action(bucket: str, key: str, request: Request) -> Any:
        """Action endpoint: ?uploads (initiate), ?uploadId=..&complete/abort."""
        principal = _principal(request)
        q = request.query_params
        if "uploads" in q:
            result = await store.initiate_upload(
                bucket, key, principal,
                content_type=request.headers.get("content-type"))
            return JSONResponse(status_code=201, content=result.model_dump())
        upload_id = q.get("uploadId")
        if not upload_id:
            raise error(ErrorCode.INVALID_ARGUMENT,
                        "expected ?uploads or ?uploadId=...&action=")
        action = q.get("action", "complete")
        if action == "abort":
            await store.abort_upload(bucket, key, upload_id, principal)
            return {"bucket": bucket, "key": key,
                    "upload_id": upload_id, "aborted": True}
        if action == "parts":
            result = await store.list_parts(bucket, key, upload_id, principal)
            return result.model_dump()
        # default: complete
        body = await _optional_json(request)
        parts = body.get("parts") if body else None
        result = await store.complete_upload(
            bucket, key, upload_id, principal, parts=parts)
        return JSONResponse(status_code=200, content=result.model_dump())

    @app.get("/v1/buckets/{bucket}/objects/{key:path}")
    async def get_object(bucket: str, key: str, request: Request) -> StreamingResponse:
        principal = _principal(request)
        meta, stream = await store.get_object(bucket, key, principal)
        return StreamingResponse(
            stream, status_code=200,
            media_type=meta.content_type or "application/octet-stream",
            headers=_etag_headers(meta) | {
                "Content-Length": str(meta.size),
                "X-OSG-Version-Id": meta.version_id,
            },
        )

    @app.head("/v1/buckets/{bucket}/objects/{key:path}")
    async def head_object(bucket: str, key: str, request: Request) -> Response:
        principal = _principal(request)
        meta = await store.head_object(bucket, key, principal)
        return Response(status_code=200, headers=_etag_headers(meta) | {
            "Content-Length": str(meta.size),
            "Content-Type": meta.content_type or "application/octet-stream",
            "X-OSG-Version-Id": meta.version_id,
        })

    @app.delete("/v1/buckets/{bucket}/objects/{key:path}")
    async def delete_object(bucket: str, key: str, request: Request) -> dict:
        principal = _principal(request)
        existed = await store.delete_object(bucket, key, principal)
        # Idempotent 200; clients can distinguish via "existed".
        return {"bucket": bucket, "key": key, "deleted": True, "existed": existed}

    return app


def _etag_headers(meta) -> dict[str, str]:
    return {"ETag": f'"{meta.etag}"', "X-OSG-Version-Id": meta.version_id}


def _object_headers(meta) -> dict:
    return meta.model_dump()


async def _optional_json(request: Request) -> dict | None:
    raw = await request.body()
    if not raw:
        return None
    import json
    try:
        data = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise error(ErrorCode.INVALID_ARGUMENT, f"invalid JSON body: {exc}")
    if not isinstance(data, dict):
        raise error(ErrorCode.INVALID_ARGUMENT, "JSON body must be an object")
    return data
