"""Business logic: authorisation + store orchestration.

Error contract:
  * Unauthorised callers always get AccessDenied (403), whether or not the
    target object exists -- an auth failure never leaks existence.
  * Authorised callers get precise 404s (NoSuchBucket / NoSuchKey /
    NoSuchUpload) and 409/412 conflicts, so every failure mode is
    distinguishable.
"""

from __future__ import annotations

from typing import Iterator

from . import auth
from .errors import AccessDenied, InvalidRequest, NoSuchBucket
from .models import BucketMeta, ObjectMeta, PartMeta, UploadMeta, VersionMeta
from .store import Store


class GatewayService:
    def __init__(self, store: Store):
        self.store = store

    # -- helpers -------------------------------------------------------------
    def _bucket_or_404(self, name: str) -> BucketMeta:
        bucket = self.store.get_bucket(name)
        if bucket is None:
            raise NoSuchBucket(f"bucket '{name}' does not exist")
        return bucket

    def _authorize(self, name: str, identity: str, action: str) -> BucketMeta:
        bucket = self._bucket_or_404(name)
        auth.require(bucket, identity, action)
        return bucket

    # -- buckets -------------------------------------------------------------
    def create_bucket(self, identity: str, name: str, policy: dict | None = None,
                      versioning_enabled: bool = False,
                      max_versions: int = 0,
                      max_version_bytes: int = 0) -> BucketMeta:
        if not name or "/" in name or name in (".", ".."):
            raise InvalidRequest(f"invalid bucket name: {name!r}")
        self._validate_retention(max_versions, max_version_bytes)
        meta = BucketMeta(name=name, owner=identity, policy=policy or {},
                          versioning_enabled=versioning_enabled,
                          max_versions=max_versions,
                          max_version_bytes=max_version_bytes)
        self.store.create_bucket(meta)
        return meta

    @staticmethod
    def _validate_retention(max_versions: int, max_version_bytes: int) -> None:
        if not isinstance(max_versions, int) or max_versions < 0:
            raise InvalidRequest("max_versions must be a non-negative integer")
        if not isinstance(max_version_bytes, int) or max_version_bytes < 0:
            raise InvalidRequest("max_version_bytes must be a non-negative integer")
        if max_versions == 1:
            raise InvalidRequest(
                "max_versions=1 would evict every old version immediately; "
                "use 0 (unlimited) or a value >= 2, or disable versioning")

    def set_versioning(self, identity: str, name: str, *,
                       enabled: bool | None = None,
                       max_versions: int | None = None,
                       max_version_bytes: int | None = None) -> BucketMeta:
        """Enable/disable historical retention and adjust its limits.
        Only the owner may change it. Disabling keeps already-kept versions
        readable; only subsequent overwrites stop preserving history."""
        bucket = self._bucket_or_404(name)
        if bucket.owner != identity:
            raise AccessDenied("only the bucket owner may change versioning")
        if enabled is not None:
            bucket.versioning_enabled = bool(enabled)
        if max_versions is not None:
            self._validate_retention(max_versions, bucket.max_version_bytes)
            bucket.max_versions = max_versions
        if max_version_bytes is not None:
            self._validate_retention(bucket.max_versions, max_version_bytes)
            bucket.max_version_bytes = max_version_bytes
        self.store.update_bucket(bucket)
        return bucket

    def list_buckets(self, identity: str) -> list[BucketMeta]:
        return [b for b in self.store.list_buckets()
                if b.owner == identity or any(
                    auth.is_allowed(b, identity, a) for a in auth.ACTIONS)]

    def get_bucket(self, identity: str, name: str) -> BucketMeta:
        return self._authorize(name, identity, "read")

    def set_policy(self, identity: str, name: str, policy: dict) -> BucketMeta:
        bucket = self._bucket_or_404(name)
        if bucket.owner != identity:
            raise AccessDenied("only the bucket owner may change the policy")
        for action in policy:
            if action not in auth.ACTIONS:
                from .errors import InvalidRequest
                raise InvalidRequest(f"unknown policy action: {action}")
        bucket.policy = policy
        self.store.update_bucket(bucket)
        return bucket

    def delete_bucket(self, identity: str, name: str) -> None:
        self._authorize(name, identity, "delete")
        self.store.delete_bucket(name)

    # -- objects -------------------------------------------------------------
    def put_object(self, identity: str, bucket: str, key: str,
                   chunks: Iterator[bytes], if_version: int | None = None,
                   if_absent: bool = False) -> ObjectMeta:
        self._authorize(bucket, identity, "write")
        return self.store.put_object(bucket, key, chunks,
                                     if_version=if_version, if_absent=if_absent)

    def get_object_meta(self, identity: str, bucket: str, key: str) -> ObjectMeta:
        self._authorize(bucket, identity, "read")
        meta = self.store.get_object_meta(bucket, key)
        if meta is None:
            from .errors import NoSuchKey
            raise NoSuchKey(f"object '{key}' does not exist in bucket '{bucket}'")
        return meta

    def read_object(self, identity: str, bucket: str, key: str) -> tuple[ObjectMeta, Iterator[bytes]]:
        meta = self.get_object_meta(identity, bucket, key)
        return meta, self.store.read_object_stream(bucket, key)

    def list_objects(self, identity: str, bucket: str, prefix: str = "",
                     marker: str = "", limit: int | None = None):
        self._authorize(bucket, identity, "list")
        return self.store.list_object_metas(bucket, prefix=prefix, marker=marker, limit=limit)

    def delete_object(self, identity: str, bucket: str, key: str) -> ObjectMeta:
        self._authorize(bucket, identity, "delete")
        return self.store.delete_object(bucket, key)

    # -- historical versions ---------------------------------------------------
    def list_versions(self, identity: str, bucket: str, key: str) -> list[dict]:
        """All versions of the key, newest first, each flagged as current."""
        self._authorize(bucket, identity, "read")
        current = self.store.get_object_meta(bucket, key)
        if current is None:
            from .errors import NoSuchKey
            raise NoSuchKey(f"object '{key}' does not exist in bucket '{bucket}'")
        versions = self.store.list_versions(bucket, key)
        out = []
        if not versions and not current.version_id:
            # legacy current: versioning never archived it, but the current
            # content is still a listable/readable "version".
            out.append(self._version_view(VersionMeta(
                key=current.key, version=current.version, version_id="",
                size=current.size, etag=current.etag,
                created_at=current.updated_at), current, legacy=True))
        for v in reversed(versions):
            out.append(self._version_view(v, current))
        return out

    @staticmethod
    def _version_view(v: VersionMeta, current: ObjectMeta, legacy: bool = False) -> dict:
        d = v.to_dict()
        d["is_current"] = legacy or (v.version_id == current.version_id)
        d["current_version"] = current.version
        return d

    def read_version(self, identity: str, bucket: str, key: str, version_id: str):
        self._authorize(bucket, identity, "read")
        current = self.store.get_object_meta(bucket, key)
        if current is None:
            from .errors import NoSuchKey
            raise NoSuchKey(f"object '{key}' does not exist in bucket '{bucket}'")
        # open + metadata are resolved under one bucket lock in the store,
        # so the returned header and body are the same immutable version.
        return self.store.read_version_stream(bucket, key, version_id)

    def rollback_version(self, identity: str, bucket: str, key: str,
                         version_id: str, if_version: int | None = None,
                         if_absent: bool = False) -> ObjectMeta:
        """Re-submit a historical version as a new current write. Requires
        write permission and goes through the normal conditional checks;
        history is never mutated."""
        bmeta = self._authorize(bucket, identity, "write")
        if not bmeta.versioning_enabled:
            raise InvalidRequest(
                "cannot roll back in a bucket with versioning disabled")
        return self.store.promote_version(bucket, bmeta, key, version_id,
                                          if_version=if_version, if_absent=if_absent)

    # -- multipart -------------------------------------------------------------
    def create_upload(self, identity: str, bucket: str, key: str) -> UploadMeta:
        self._authorize(bucket, identity, "write")
        return self.store.create_upload(bucket, key, identity)

    def list_uploads(self, identity: str, bucket: str) -> list[UploadMeta]:
        self._authorize(bucket, identity, "read")
        return self.store.list_uploads(bucket)

    def put_part(self, identity: str, bucket: str, upload_id: str, number: int,
                 chunks: Iterator[bytes]) -> PartMeta:
        self._authorize(bucket, identity, "write")
        return self.store.put_part(bucket, upload_id, number, chunks)

    def complete_upload(self, identity: str, bucket: str, upload_id: str,
                        if_version: int | None = None,
                        if_absent: bool = False) -> ObjectMeta:
        self._authorize(bucket, identity, "write")
        return self.store.complete_upload(bucket, upload_id,
                                          if_version=if_version, if_absent=if_absent)

    def abort_upload(self, identity: str, bucket: str, upload_id: str) -> None:
        self._authorize(bucket, identity, "write")
        self.store.abort_upload(bucket, upload_id)
