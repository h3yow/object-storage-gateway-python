"""Filesystem-backed metadata + data store.

Layout under data_dir:

    buckets/<bucket>/bucket.json                 BucketMeta
    buckets/<bucket>/objects/<hash>.json         ObjectMeta   (committed last)
    buckets/<bucket>/objects-data/<hash>         object bytes (written first)
    buckets/<bucket>/uploads/<upload_id>.json    UploadMeta
    buckets/<bucket>/uploads-data/<id>/<part>    part bytes

Consistency rules:
  * Every metadata write is tmp-file + fsync + os.replace (atomic rename).
  * Object data is committed *before* its metadata, so a crash can leave an
    orphan data file but never metadata pointing at missing data.
  * Object delete removes metadata *first*, so a crash leaves orphan data,
    again recoverable.
  * recover() removes orphan data files and stale tmp files; in-flight
    multipart uploads are kept (they are resumable by design).

Concurrency: a global lock guards the bucket registry and quota accounting;
a per-bucket reentrant lock serialises all mutations within a bucket, which
serialises concurrent writes to the same object.
"""

from __future__ import annotations

import hashlib
import json
import os
import threading
from pathlib import Path
from typing import Iterator

from .config import Settings
from .errors import (BucketAlreadyExists, BucketNotEmpty, EntityTooLarge,
                     InvalidPart, NoSuchBucket, NoSuchKey, NoSuchUpload,
                     QuotaExceeded, TooManyRequests)
from .models import BucketMeta, ObjectMeta, PartMeta, UploadMeta, new_id

TMP_SUFFIX = ".tmp"


def _key_hash(key: str) -> str:
    return hashlib.sha256(key.encode("utf-8")).hexdigest()


def _atomic_write_json(path: Path, payload: dict) -> None:
    tmp = path.with_name(path.name + TMP_SUFFIX)
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(payload, f)
        f.flush()
        os.fsync(f.fileno())
    os.replace(tmp, path)


def _read_json(path: Path) -> dict:
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


class Store:
    def __init__(self, settings: Settings):
        self.settings = settings
        self.root = settings.data_dir
        self._global_lock = threading.RLock()
        self._bucket_locks: dict[str, threading.RLock] = {}
        self.settings.ensure_dirs()
        (self.root / "buckets").mkdir(parents=True, exist_ok=True)

    # -- paths -------------------------------------------------------------
    def _bucket_dir(self, bucket: str) -> Path:
        return self.root / "buckets" / bucket

    def _bucket_meta_path(self, bucket: str) -> Path:
        return self._bucket_dir(bucket) / "bucket.json"

    def _objects_dir(self, bucket: str) -> Path:
        return self._bucket_dir(bucket) / "objects"

    def _objects_data_dir(self, bucket: str) -> Path:
        return self._bucket_dir(bucket) / "objects-data"

    def _uploads_dir(self, bucket: str) -> Path:
        return self._bucket_dir(bucket) / "uploads"

    def _uploads_data_dir(self, bucket: str) -> Path:
        return self._bucket_dir(bucket) / "uploads-data"

    def _bucket_lock(self, bucket: str) -> threading.RLock:
        with self._global_lock:
            return self._bucket_locks.setdefault(bucket, threading.RLock())

    # -- recovery ------------------------------------------------------------
    def recover(self) -> dict:
        """Remove orphan data files and stale tmp files. Returns a report."""
        report = {"orphan_data_removed": 0, "tmp_removed": 0, "orphan_parts_removed": 0}
        buckets_dir = self.root / "buckets"
        if not buckets_dir.exists():
            return report
        for bucket_dir in buckets_dir.iterdir():
            if not bucket_dir.is_dir():
                continue
            # stale tmp files anywhere under the bucket
            for tmp in bucket_dir.rglob(f"*{TMP_SUFFIX}"):
                tmp.unlink()
                report["tmp_removed"] += 1
            objects_dir = bucket_dir / "objects"
            data_dir = bucket_dir / "objects-data"
            metas = {p.stem for p in objects_dir.glob("*.json")} if objects_dir.exists() else set()
            if data_dir.exists():
                for data_file in data_dir.iterdir():
                    if data_file.name not in metas:
                        data_file.unlink()
                        report["orphan_data_removed"] += 1
            uploads_dir = bucket_dir / "uploads"
            uploads_data = bucket_dir / "uploads-data"
            upload_ids = {p.stem for p in uploads_dir.glob("*.json")} if uploads_dir.exists() else set()
            if uploads_data.exists():
                for up_dir in uploads_data.iterdir():
                    if up_dir.name not in upload_ids:
                        for part in up_dir.iterdir():
                            part.unlink()
                        up_dir.rmdir()
                        report["orphan_parts_removed"] += 1
        return report

    # -- buckets -------------------------------------------------------------
    def create_bucket(self, meta: BucketMeta) -> None:
        with self._global_lock:
            path = self._bucket_meta_path(meta.name)
            if path.exists():
                raise BucketAlreadyExists(f"bucket '{meta.name}' already exists")
            self._bucket_dir(meta.name).mkdir(parents=True, exist_ok=True)
            self._objects_dir(meta.name).mkdir(exist_ok=True)
            self._objects_data_dir(meta.name).mkdir(exist_ok=True)
            self._uploads_dir(meta.name).mkdir(exist_ok=True)
            self._uploads_data_dir(meta.name).mkdir(exist_ok=True)
            _atomic_write_json(path, meta.to_dict())

    def get_bucket(self, name: str) -> BucketMeta | None:
        path = self._bucket_meta_path(name)
        if not path.exists():
            return None
        return BucketMeta.from_dict(_read_json(path))

    def update_bucket(self, meta: BucketMeta) -> None:
        with self._bucket_lock(meta.name):
            if not self._bucket_meta_path(meta.name).exists():
                raise NoSuchBucket(f"bucket '{meta.name}' does not exist")
            _atomic_write_json(self._bucket_meta_path(meta.name), meta.to_dict())

    def list_buckets(self) -> list[BucketMeta]:
        buckets_dir = self.root / "buckets"
        out = []
        for meta_path in sorted(buckets_dir.glob("*/bucket.json")):
            out.append(BucketMeta.from_dict(_read_json(meta_path)))
        return out

    def delete_bucket(self, name: str) -> None:
        with self._bucket_lock(name):
            bucket_dir = self._bucket_dir(name)
            if not self._bucket_meta_path(name).exists():
                raise NoSuchBucket(f"bucket '{name}' does not exist")
            if any(self._objects_dir(name).glob("*.json")):
                raise BucketNotEmpty(f"bucket '{name}' is not empty")
            # abort any in-flight uploads so no residue remains
            for up in self._uploads_dir(name).glob("*.json"):
                up.unlink()
            up_data = self._uploads_data_dir(name)
            if up_data.exists():
                for d in up_data.iterdir():
                    for p in d.iterdir():
                        p.unlink()
                    d.rmdir()
            for sub in ("objects", "objects-data", "uploads", "uploads-data"):
                (bucket_dir / sub).rmdir()
            self._bucket_meta_path(name).unlink()
            bucket_dir.rmdir()

    # -- quota -----------------------------------------------------------------
    def _usage_bytes_locked(self) -> int:
        total = 0
        for meta_path in (self.root / "buckets").glob("*/objects/*.json"):
            total += int(_read_json(meta_path)["size"])
        for part in (self.root / "buckets").glob("*/uploads-data/*/*"):
            total += part.stat().st_size
        return total

    def _check_quota_locked(self, additional: int) -> None:
        quota = self.settings.quota_bytes
        if quota and self._usage_bytes_locked() + additional > quota:
            raise QuotaExceeded(
                f"quota of {quota} bytes would be exceeded by {additional} more bytes")

    def usage_bytes(self) -> int:
        with self._global_lock:
            return self._usage_bytes_locked()

    # -- objects ---------------------------------------------------------------
    def get_object_meta(self, bucket: str, key: str) -> ObjectMeta | None:
        path = self._objects_dir(bucket) / f"{_key_hash(key)}.json"
        if not path.exists():
            return None
        return ObjectMeta.from_dict(_read_json(path))

    def list_object_metas(self, bucket: str, prefix: str = "", marker: str = "",
                          limit: int | None = None) -> tuple[list[ObjectMeta], str | None]:
        """Returns (metas sorted by key, next_marker). Bounded by limit."""
        limit = limit if limit is not None else self.settings.max_list_keys
        limit = min(limit, self.settings.max_list_keys)
        with self._bucket_lock(bucket):
            metas = []
            for path in self._objects_dir(bucket).glob("*.json"):
                meta = ObjectMeta.from_dict(_read_json(path))
                if meta.key.startswith(prefix) and meta.key > marker:
                    metas.append(meta)
            metas.sort(key=lambda m: m.key)
            truncated = len(metas) > limit
            page = metas[:limit]
            next_marker = page[-1].key if truncated and page else None
            return page, next_marker

    def put_object(self, bucket: str, key: str, chunks: Iterator[bytes],
                   if_version: int | None = None,
                   if_absent: bool = False) -> ObjectMeta:
        """Stream chunks to disk, then commit metadata atomically.

        Conditional semantics (evaluated under the bucket lock):
          * if_absent: fail with Conflict if the key already exists.
          * if_version: fail with PreconditionFailed unless the current
            version matches exactly.
        """
        with self._global_lock, self._bucket_lock(bucket):
            existing = self.get_object_meta(bucket, key)
            if if_absent and existing is not None:
                from .errors import Conflict
                raise Conflict(f"object '{key}' already exists")
            if if_version is not None:
                current = existing.version if existing else 0
                if current != if_version:
                    from .errors import PreconditionFailed
                    raise PreconditionFailed(
                        f"expected version {if_version}, current version is {current}")
            h = _key_hash(key)
            data_path = self._objects_data_dir(bucket) / h
            tmp_path = data_path.with_name(data_path.name + TMP_SUFFIX)
            digest = hashlib.sha256()
            size = 0
            try:
                with open(tmp_path, "wb") as f:
                    for chunk in chunks:
                        size += len(chunk)
                        if size > self.settings.max_object_size:
                            raise EntityTooLarge(
                                f"object exceeds max size {self.settings.max_object_size}")
                        digest.update(chunk)
                        f.write(chunk)
                    f.flush()
                    os.fsync(f.fileno())
            except BaseException:
                tmp_path.unlink(missing_ok=True)
                raise
            try:
                self._check_quota_locked(size - (existing.size if existing else 0))
            except BaseException:
                tmp_path.unlink(missing_ok=True)
                raise
            os.replace(tmp_path, data_path)  # data committed first
            now = None
            meta = ObjectMeta(key=key, size=size, etag=digest.hexdigest(),
                              version=(existing.version + 1) if existing else 1)
            if existing:
                meta.created_at = existing.created_at
            _atomic_write_json(self._objects_dir(bucket) / f"{h}.json", meta.to_dict())
            return meta

    def read_object_stream(self, bucket: str, key: str) -> Iterator[bytes]:
        """Yield object bytes in bounded chunks; never loads it all."""
        meta = self.get_object_meta(bucket, key)
        if meta is None:
            raise NoSuchKey(f"object '{key}' does not exist in bucket '{bucket}'")
        data_path = self._objects_data_dir(bucket) / _key_hash(key)
        chunk = self.settings.stream_chunk_size
        with open(data_path, "rb") as f:
            while True:
                buf = f.read(chunk)
                if not buf:
                    break
                yield buf

    def delete_object(self, bucket: str, key: str) -> ObjectMeta:
        with self._global_lock, self._bucket_lock(bucket):
            meta = self.get_object_meta(bucket, key)
            if meta is None:
                raise NoSuchKey(f"object '{key}' does not exist in bucket '{bucket}'")
            # metadata first: a crash leaves recoverable orphan data, never
            # metadata without a body.
            (self._objects_dir(bucket) / f"{_key_hash(key)}.json").unlink()
            (self._objects_data_dir(bucket) / _key_hash(key)).unlink(missing_ok=True)
            return meta

    # -- multipart uploads -------------------------------------------------------
    def create_upload(self, bucket: str, key: str, initiator: str) -> UploadMeta:
        with self._global_lock, self._bucket_lock(bucket):
            if self.settings.max_active_uploads:
                active = len(list(self._uploads_dir(bucket).glob("*.json")))
                if active >= self.settings.max_active_uploads:
                    raise TooManyRequests(
                        f"too many active uploads in bucket '{bucket}' "
                        f"(limit {self.settings.max_active_uploads})")
            upload = UploadMeta(upload_id=new_id(), key=key, initiator=initiator)
            (self._uploads_data_dir(bucket) / upload.upload_id).mkdir(parents=True, exist_ok=True)
            _atomic_write_json(self._uploads_dir(bucket) / f"{upload.upload_id}.json",
                               upload.to_dict())
            return upload

    def get_upload(self, bucket: str, upload_id: str) -> UploadMeta | None:
        path = self._uploads_dir(bucket) / f"{upload_id}.json"
        if not path.exists():
            return None
        return UploadMeta.from_dict(_read_json(path))

    def list_uploads(self, bucket: str) -> list[UploadMeta]:
        with self._bucket_lock(bucket):
            return [UploadMeta.from_dict(_read_json(p))
                    for p in sorted(self._uploads_dir(bucket).glob("*.json"))]

    def put_part(self, bucket: str, upload_id: str, number: int,
                 chunks: Iterator[bytes]) -> PartMeta:
        """Store (or idempotently replace) one part. Re-transmitting the same
        part number never double-counts: the old file is replaced."""
        if number < 1 or number > 10000:
            raise InvalidPart(f"part number {number} out of range 1..10000")
        with self._global_lock, self._bucket_lock(bucket):
            upload = self.get_upload(bucket, upload_id)
            if upload is None:
                raise NoSuchUpload(f"upload '{upload_id}' does not exist")
            part_path = self._uploads_data_dir(bucket) / upload_id / str(number)
            tmp_path = part_path.with_name(part_path.name + TMP_SUFFIX)
            digest = hashlib.sha256()
            size = 0
            try:
                with open(tmp_path, "wb") as f:
                    for chunk in chunks:
                        size += len(chunk)
                        if size > self.settings.max_object_size:
                            raise EntityTooLarge("part exceeds max object size")
                        digest.update(chunk)
                        f.write(chunk)
                    f.flush()
                    os.fsync(f.fileno())
            except BaseException:
                tmp_path.unlink(missing_ok=True)
                raise
            old_size = upload.parts.get(str(number))
            try:
                self._check_quota_locked(size - (old_size.size if old_size else 0))
            except BaseException:
                tmp_path.unlink(missing_ok=True)
                raise
            os.replace(tmp_path, part_path)
            part = PartMeta(number=number, size=size, etag=digest.hexdigest())
            upload.parts[str(number)] = part
            _atomic_write_json(self._uploads_dir(bucket) / f"{upload_id}.json",
                               upload.to_dict())
            return part

    def complete_upload(self, bucket: str, upload_id: str,
                        if_version: int | None = None,
                        if_absent: bool = False) -> ObjectMeta:
        """Assemble parts in order into a committed object. Atomic: the new
        object becomes visible only after all parts are merged and metadata
        is committed. Upload state is removed only after the commit."""
        with self._global_lock, self._bucket_lock(bucket):
            upload = self.get_upload(bucket, upload_id)
            if upload is None:
                raise NoSuchUpload(f"upload '{upload_id}' does not exist")
            if not upload.parts:
                raise InvalidPart("upload has no parts")
            parts = sorted(upload.parts.values(), key=lambda p: p.number)
            total = sum(p.size for p in parts)
            if total > self.settings.max_object_size:
                raise EntityTooLarge(
                    f"merged object of {total} bytes exceeds max size "
                    f"{self.settings.max_object_size}")
            key = upload.key
            existing = self.get_object_meta(bucket, key)
            if if_absent and existing is not None:
                from .errors import Conflict
                raise Conflict(f"object '{key}' already exists")
            if if_version is not None:
                current = existing.version if existing else 0
                if current != if_version:
                    from .errors import PreconditionFailed
                    raise PreconditionFailed(
                        f"expected version {if_version}, current version is {current}")
            h = _key_hash(key)
            data_path = self._objects_data_dir(bucket) / h
            tmp_path = data_path.with_name(data_path.name + TMP_SUFFIX)
            digest = hashlib.sha256()
            try:
                with open(tmp_path, "wb") as out:
                    for part in parts:
                        part_path = (self._uploads_data_dir(bucket) / upload_id
                                     / str(part.number))
                        with open(part_path, "rb") as pf:
                            while True:
                                buf = pf.read(self.settings.stream_chunk_size)
                                if not buf:
                                    break
                                digest.update(buf)
                                out.write(buf)
                    out.flush()
                    os.fsync(out.fileno())
            except BaseException:
                tmp_path.unlink(missing_ok=True)
                raise
            try:
                self._check_quota_locked(total - (existing.size if existing else 0))
            except BaseException:
                tmp_path.unlink(missing_ok=True)
                raise
            os.replace(tmp_path, data_path)
            meta = ObjectMeta(key=key, size=total, etag=digest.hexdigest(),
                              version=(existing.version + 1) if existing else 1)
            if existing:
                meta.created_at = existing.created_at
            _atomic_write_json(self._objects_dir(bucket) / f"{h}.json", meta.to_dict())
            # object committed; now remove upload state
            self._remove_upload_locked(bucket, upload_id)
            return meta

    def abort_upload(self, bucket: str, upload_id: str) -> None:
        with self._global_lock, self._bucket_lock(bucket):
            if self.get_upload(bucket, upload_id) is None:
                raise NoSuchUpload(f"upload '{upload_id}' does not exist")
            self._remove_upload_locked(bucket, upload_id)

    def _remove_upload_locked(self, bucket: str, upload_id: str) -> None:
        (self._uploads_dir(bucket) / f"{upload_id}.json").unlink(missing_ok=True)
        data_dir = self._uploads_data_dir(bucket) / upload_id
        if data_dir.exists():
            for part in data_dir.iterdir():
                part.unlink()
            data_dir.rmdir()
