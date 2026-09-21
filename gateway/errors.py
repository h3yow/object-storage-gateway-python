"""Stable error taxonomy for the gateway.

Every failure raised by the service layer is a GatewayError carrying a
machine-readable ``code``, an HTTP ``status`` and a human readable message.
The API layer maps these 1:1 onto the response envelope so clients see a
stable contract.
"""

from __future__ import annotations


class GatewayError(Exception):
    """Base class for all gateway errors."""

    code: str = "InternalError"
    status: int = 500

    def __init__(self, message: str = "", *, detail: dict | None = None):
        super().__init__(message or self.code)
        self.message = message or self.code
        self.detail = detail or {}

    def to_dict(self) -> dict:
        return {"error": {"code": self.code, "message": self.message, "detail": self.detail}}


# --- 404 family -----------------------------------------------------------
class NoSuchBucket(GatewayError):
    code = "NoSuchBucket"
    status = 404


class NoSuchKey(GatewayError):
    code = "NoSuchKey"
    status = 404


class NoSuchUpload(GatewayError):
    code = "NoSuchUpload"
    status = 404


# --- 403 family -----------------------------------------------------------
class AccessDenied(GatewayError):
    code = "AccessDenied"
    status = 403


# --- 409 family -----------------------------------------------------------
class BucketAlreadyExists(GatewayError):
    code = "BucketAlreadyExists"
    status = 409


class BucketNotEmpty(GatewayError):
    code = "BucketNotEmpty"
    status = 409


class Conflict(GatewayError):
    """Generic state conflict, e.g. conditional-write precondition failed."""

    code = "Conflict"
    status = 409


class PreconditionFailed(GatewayError):
    code = "PreconditionFailed"
    status = 412


# --- 400 family -----------------------------------------------------------
class InvalidRequest(GatewayError):
    code = "InvalidRequest"
    status = 400


class InvalidPart(GatewayError):
    code = "InvalidPart"
    status = 400


# --- 4xx quota family -----------------------------------------------------
class QuotaExceeded(GatewayError):
    code = "QuotaExceeded"
    status = 403


class EntityTooLarge(GatewayError):
    code = "EntityTooLarge"
    status = 413


class TooManyRequests(GatewayError):
    code = "TooManyRequests"
    status = 429
