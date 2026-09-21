"""Pydantic request/response models (wire format is stable)."""
from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, Field

# ---- bucket policy ----

Action = Literal["s3:GetObject", "s3:PutObject", "s3:DeleteObject",
                 "s3:ListBucket", "s3:CreateBucket", "s3:DeleteBucket"]
Effect = Literal["allow", "deny"]


class PolicyStatement(BaseModel):
    effect: Effect
    principal: str
    actions: list[str]


class BucketPolicy(BaseModel):
    statements: list[PolicyStatement] = Field(default_factory=list)


# ---- object ----

class ObjectMeta(BaseModel):
    bucket: str
    key: str
    size: int
    etag: str
    version_id: str
    created_at: float
    updated_at: float
    content_type: str | None = None


class ListObjectsResult(BaseModel):
    bucket: str
    prefix: str
    delimiter: str | None = None
    next_marker: str | None = None
    truncated: bool
    objects: list[ObjectMeta]
    common_prefixes: list[str] = Field(default_factory=list)


# ---- multipart ----

class InitiateUploadResult(BaseModel):
    bucket: str
    key: str
    upload_id: str


class PartInfo(BaseModel):
    part_number: int
    size: int
    etag: str


class CompleteUploadResult(BaseModel):
    bucket: str
    key: str
    etag: str
    version_id: str
    size: int


class AbortUploadResult(BaseModel):
    bucket: str
    key: str
    upload_id: str


class ListPartsResult(BaseModel):
    bucket: str
    key: str
    upload_id: str
    parts: list[PartInfo]
