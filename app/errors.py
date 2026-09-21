"""Stable error classification.

Every failure maps to a distinct ErrorCode + HTTP status. Wire envelope:

    {"error": {"code": "...", "message": "...", "request_id": "..."}}

Authentication/authorization failures always return ACCESS_DENIED, *before*
existence is checked, so a denied principal cannot learn whether a resource
exists.
"""
from __future__ import annotations

from enum import Enum

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse


class ErrorCode(str, Enum):
    ACCESS_DENIED = "AccessDenied"
    UNAUTHORIZED = "Unauthorized"
    BUCKET_NOT_EMPTY = "BucketNotEmpty"
    BUCKET_NOT_FOUND = "NoSuchBucket"
    OBJECT_NOT_FOUND = "NoSuchKey"
    UPLOAD_NOT_FOUND = "NoSuchUpload"
    BUCKET_EXISTS = "BucketAlreadyExists"
    ENTITY_TOO_LARGE = "EntityTooLarge"
    QUOTA_EXCEEDED = "QuotaExceeded"
    PRECONDITION_FAILED = "PreconditionFailed"
    CONFLICT = "Conflict"
    INVALID_ARGUMENT = "InvalidArgument"
    INVALID_PART = "InvalidPart"
    INVALID_PART_ORDER = "InvalidPartOrder"
    NO_SUCH_BUCKET_POLICY = "NoSuchBucketPolicy"
    INTERNAL = "InternalError"


# HTTP status per code (kept explicit so the mapping never drifts)
_STATUS = {
    ErrorCode.ACCESS_DENIED: 403,
    ErrorCode.UNAUTHORIZED: 401,
    ErrorCode.BUCKET_NOT_EMPTY: 409,
    ErrorCode.BUCKET_NOT_FOUND: 404,
    ErrorCode.OBJECT_NOT_FOUND: 404,
    ErrorCode.UPLOAD_NOT_FOUND: 404,
    ErrorCode.BUCKET_EXISTS: 409,
    ErrorCode.ENTITY_TOO_LARGE: 413,
    ErrorCode.QUOTA_EXCEEDED: 507,
    ErrorCode.PRECONDITION_FAILED: 412,
    ErrorCode.CONFLICT: 409,
    ErrorCode.INVALID_ARGUMENT: 400,
    ErrorCode.INVALID_PART: 400,
    ErrorCode.INVALID_PART_ORDER: 400,
    ErrorCode.NO_SUCH_BUCKET_POLICY: 404,
    ErrorCode.INTERNAL: 500,
}


class GatewayError(Exception):
    def __init__(self, code: ErrorCode, message: str,
                 status_code: int | None = None) -> None:
        super().__init__(message)
        self.code = code
        self.message = message
        self.status_code = status_code or _STATUS[code]

    def envelope(self, request_id: str | None = None) -> dict:
        return {"error": {
            "code": self.code.value,
            "message": self.message,
            "request_id": request_id,
        }}


def error(code: ErrorCode, message: str) -> GatewayError:
    return GatewayError(code, message)


def register_exception_handlers(app: FastAPI) -> None:
    @app.exception_handler(GatewayError)
    async def _on_gateway_error(request: Request, exc: GatewayError) -> JSONResponse:
        request_id = getattr(request.state, "request_id", None)
        return JSONResponse(
            status_code=exc.status_code,
            content=exc.envelope(request_id),
            headers={"Cache-Control": "no-store"},
        )

    @app.exception_handler(Exception)
    async def _on_unexpected(request: Request, exc: Exception) -> JSONResponse:
        request_id = getattr(request.state, "request_id", None)
        gw = GatewayError(ErrorCode.INTERNAL, "internal server error")
        return JSONResponse(
            status_code=500,
            content=gw.envelope(request_id),
            headers={"Cache-Control": "no-store"},
        )
