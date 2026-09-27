"""Local object storage gateway -- HTTP API layer.

Thin layer over GatewayService: parses identity/params, streams bodies,
and maps GatewayError onto a stable JSON error envelope.
"""

from __future__ import annotations

import asyncio

import anyio
from fastapi import FastAPI, Query, Request
from fastapi.responses import JSONResponse, StreamingResponse

from gateway.config import Settings
from gateway.errors import GatewayError
from gateway.service import GatewayService
from gateway.store import Store
from gateway.streaming import sync_chunks
from gateway import auth


def create_app(settings: Settings | None = None) -> FastAPI:
    settings = settings or Settings()
    store = Store(settings)
    recovery_report = store.recover()
    service = GatewayService(store)

    app = FastAPI(title="object-storage-gateway")
    app.state.service = service
    app.state.settings = settings
    app.state.recovery_report = recovery_report

    @app.exception_handler(GatewayError)
    async def gateway_error_handler(_req: Request, exc: GatewayError):
        return JSONResponse(status_code=exc.status, content=exc.to_dict())

    def identity(request: Request) -> str:
        return auth.parse_identity(request.headers)

    # -- buckets -------------------------------------------------------------
    @app.put("/buckets/{bucket}")
    def create_bucket(bucket: str, request: Request, body: dict | None = None):
        body = body or {}
        meta = service.create_bucket(
            identity(request), bucket, policy=body.get("policy"),
            versioning_enabled=bool(body.get("versioning_enabled", False)),
            max_versions=int(body.get("max_versions", 0)),
            max_version_bytes=int(body.get("max_version_bytes", 0)))
        return {"bucket": meta.to_dict()}

    @app.get("/buckets")
    def list_buckets(request: Request):
        return {"buckets": [b.to_dict() for b in service.list_buckets(identity(request))]}

    @app.get("/buckets/{bucket}")
    def get_bucket(bucket: str, request: Request):
        return {"bucket": service.get_bucket(identity(request), bucket).to_dict()}

    @app.put("/buckets/{bucket}/policy")
    def set_policy(bucket: str, request: Request, body: dict):
        meta = service.set_policy(identity(request), bucket, body.get("policy", {}))
        return {"bucket": meta.to_dict()}

    @app.put("/buckets/{bucket}/versioning")
    def set_versioning(bucket: str, request: Request, body: dict | None = None):
        body = body or {}
        if "enabled" not in body:
            from gateway.errors import InvalidRequest
            raise InvalidRequest("body requires boolean 'enabled'")
        meta = service.set_versioning(
            identity(request), bucket, bool(body["enabled"]),
            max_versions=body.get("max_versions"),
            max_version_bytes=body.get("max_version_bytes"))
        return {"bucket": meta.to_dict()}

    @app.delete("/buckets/{bucket}")
    def delete_bucket(bucket: str, request: Request):
        service.delete_bucket(identity(request), bucket)
        return {"deleted": bucket}

    # -- object version history -----------------------------------------------
    # Registered before the greedy {key:path} object routes so the extra
    # path segments are captured here.
    @app.get("/buckets/{bucket}/objects/{key:path}/versions")
    def list_versions(bucket: str, key: str, request: Request):
        current_id, versions = service.list_versions(identity(request), bucket, key)
        return {"key": key, "current_version_id": current_id,
                "versions": [
                    {**v.to_dict(), "is_current": v.version_id == current_id}
                    for v in versions]}

    @app.post("/buckets/{bucket}/objects/{key:path}/versions/{version_id}/restore")
    def restore_version(bucket: str, key: str, version_id: str, request: Request,
                        if_version: int | None = Query(default=None),
                        if_absent: bool = Query(default=False)):
        meta = service.restore_version(identity(request), bucket, key, version_id,
                                       if_version=if_version, if_absent=if_absent)
        return {"object": meta.to_dict()}

    # -- objects ---------------------------------------------------------------
    @app.put("/buckets/{bucket}/objects/{key:path}")
    async def put_object(bucket: str, key: str, request: Request,
                         if_version: int | None = Query(default=None),
                         if_absent: bool = Query(default=False)):
        who = identity(request)
        chunks = sync_chunks(request.stream(), asyncio.get_running_loop())
        meta = await anyio.to_thread.run_sync(
            lambda: service.put_object(who, bucket, key, chunks,
                                       if_version=if_version, if_absent=if_absent))
        return {"object": meta.to_dict()}

    @app.get("/buckets/{bucket}/objects/{key:path}")
    def get_object(bucket: str, key: str, request: Request,
                   version_id: str | None = Query(default=None)):
        meta, stream = service.read_object(identity(request), bucket, key,
                                           version_id=version_id)
        return StreamingResponse(stream, media_type="application/octet-stream",
                                 headers={"X-ETag": meta.etag,
                                          "X-Version": str(meta.version),
                                          "X-Version-Id": meta.version_id,
                                          "Content-Length": str(meta.size)})

    @app.head("/buckets/{bucket}/objects/{key:path}")
    def head_object(bucket: str, key: str, request: Request):
        meta = service.get_object_meta(identity(request), bucket, key)
        return JSONResponse(status_code=200, content=None,
                            headers={"X-ETag": meta.etag,
                                     "X-Version": str(meta.version),
                                     "X-Version-Id": meta.version_id,
                                     "Content-Length": str(meta.size)})

    @app.get("/buckets/{bucket}/objects")
    def list_objects(bucket: str, request: Request,
                     prefix: str = "", marker: str = "",
                     limit: int | None = Query(default=None)):
        metas, next_marker = service.list_objects(identity(request), bucket,
                                                  prefix=prefix, marker=marker,
                                                  limit=limit)
        return {"objects": [m.to_dict() for m in metas],
                "next_marker": next_marker,
                "truncated": next_marker is not None}

    @app.delete("/buckets/{bucket}/objects/{key:path}")
    def delete_object(bucket: str, key: str, request: Request):
        meta = service.delete_object(identity(request), bucket, key)
        return {"deleted": key, "version": meta.version}

    # -- multipart uploads -------------------------------------------------------
    @app.post("/buckets/{bucket}/uploads")
    def create_upload(bucket: str, request: Request, key: str = Query(...)):
        upload = service.create_upload(identity(request), bucket, key)
        return {"upload": upload.to_dict()}

    @app.get("/buckets/{bucket}/uploads")
    def list_uploads(bucket: str, request: Request):
        return {"uploads": [u.to_dict() for u in service.list_uploads(identity(request), bucket)]}

    @app.put("/buckets/{bucket}/uploads/{upload_id}/parts/{number}")
    async def put_part(bucket: str, upload_id: str, number: int, request: Request):
        who = identity(request)
        chunks = sync_chunks(request.stream(), asyncio.get_running_loop())
        part = await anyio.to_thread.run_sync(
            lambda: service.put_part(who, bucket, upload_id, number, chunks))
        return {"part": part.to_dict()}

    @app.post("/buckets/{bucket}/uploads/{upload_id}/complete")
    def complete_upload(bucket: str, upload_id: str, request: Request,
                        if_version: int | None = Query(default=None),
                        if_absent: bool = Query(default=False)):
        meta = service.complete_upload(identity(request), bucket, upload_id,
                                       if_version=if_version, if_absent=if_absent)
        return {"object": meta.to_dict()}

    @app.delete("/buckets/{bucket}/uploads/{upload_id}")
    def abort_upload(bucket: str, upload_id: str, request: Request):
        service.abort_upload(identity(request), bucket, upload_id)
        return {"aborted": upload_id}

    @app.get("/healthz")
    def healthz():
        return {"ok": True, "recovery": recovery_report}

    return app


app = create_app()
