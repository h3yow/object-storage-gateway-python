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
from .errors import AccessDenied, NoSuchBucket
from .models import BucketMeta, ObjectMeta, PartMeta, UploadMeta
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
                      versioning_enabled: bool = False, max_versions: int = 0,
                      max_version_bytes: int = 0) -> BucketMeta:
        if not name or "/" in name or name in (".", ".."):
            from .errors import InvalidRequest
            raise InvalidRequest(f"invalid bucket name: {name!r}")
        if max_versions < 0 or max_version_bytes < 0:
            from .errors import InvalidRequest
            raise InvalidRequest("retention caps must be >= 0")
        meta = BucketMeta(name=name, owner=identity, policy=policy or {},
                          versioning_enabled=versioning_enabled,
                          max_versions=max_versions,
                          max_version_bytes=max_version_bytes)
        self.store.create_bucket(meta)
        return meta

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

    def set_versioning(self, identity: str, name: str, enabled: bool,
                       max_versions: int | None = None,
                       max_version_bytes: int | None = None) -> BucketMeta:
        bucket = self._bucket_or_404(name)
        if bucket.owner != identity:
            raise AccessDenied("only the bucket owner may change versioning")
        return self.store.set_versioning(
            name, enabled, max_versions=max_versions,
            max_version_bytes=max_version_bytes)

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

    def read_object(self, identity: str, bucket: str, key: str,
                    version_id: str | None = None) -> tuple[ObjectMeta, Iterator[bytes]]:
        self._authorize(bucket, identity, "read")
        meta, f = self.store.open_object_data(bucket, key, version_id=version_id)

        def _stream():
            try:
                while True:
                    buf = f.read(self.store.settings.stream_chunk_size)
                    if not buf:
                        break
                    yield buf
            finally:
                f.close()

        return meta, _stream()

    def list_versions(self, identity: str, bucket: str, key: str):
        self._authorize(bucket, identity, "read")
        # store raises NoSuchKey / NoSuchVersion as appropriate
        return self.store.list_versions(bucket, key)

    def restore_version(self, identity: str, bucket: str, key: str,
                        version_id: str, if_version: int | None = None,
                        if_absent: bool = False) -> ObjectMeta:
        # Restore is a new write: write permission and the ordinary
        # conditional-write rules apply.
        self._authorize(bucket, identity, "write")
        return self.store.restore_version(
            bucket, key, version_id, if_version=if_version, if_absent=if_absent)

    def list_objects(self, identity: str, bucket: str, prefix: str = "",
                     marker: str = "", limit: int | None = None):
        self._authorize(bucket, identity, "list")
        return self.store.list_object_metas(bucket, prefix=prefix, marker=marker, limit=limit)

    def delete_object(self, identity: str, bucket: str, key: str) -> ObjectMeta:
        self._authorize(bucket, identity, "delete")
        return self.store.delete_object(bucket, key)

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
