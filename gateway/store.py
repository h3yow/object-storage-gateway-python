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
import shutil
import threading
import time
from pathlib import Path
from typing import BinaryIO, Iterator

from .config import Settings
from .errors import (BucketAlreadyExists, BucketNotEmpty, EntityTooLarge,
                     InvalidPart, NoSuchBucket, NoSuchKey, NoSuchUpload,
                     NoSuchVersion, QuotaExceeded, TooManyRequests)
from .models import (BucketMeta, ObjectMeta, PartMeta, UploadMeta,
                     VersionRecord, new_id)

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

    def _versions_dir(self, bucket: str) -> Path:
        return self._bucket_dir(bucket) / "versions"

    def _version_index_path(self, bucket: str, key: str) -> Path:
        h = _key_hash(key)
        return self._versions_dir(bucket) / h[:2] / f"{h}.json"

    def _version_data_path(self, bucket: str, version_id: str) -> Path:
        return self._objects_data_dir(bucket) / version_id[:2] / version_id

    def _bucket_lock(self, bucket: str) -> threading.RLock:
        with self._global_lock:
            return self._bucket_locks.setdefault(bucket, threading.RLock())

    # -- recovery ------------------------------------------------------------
    def recover(self) -> dict:
        """Reconcile on-disk state after a crash.

        Ordering invariants preserved by commits (data -> index -> pointer)
        mean every anomaly is an *orphan* (data/index without a live parent),
        never a dangling reference; recovery only removes orphans and
        finishes idempotent layout migrations. Returns a report.
        """
        report = {"orphan_data_removed": 0, "tmp_removed": 0,
                  "orphan_parts_removed": 0, "orphan_index_removed": 0,
                  "migrations_completed": 0}
        buckets_dir = self.root / "buckets"
        if not buckets_dir.exists():
            return report
        for bucket_dir in buckets_dir.iterdir():
            if not bucket_dir.is_dir():
                continue
            # stale tmp files anywhere under the bucket (incl. versioned
            # objects-data/tmp/ shards)
            for tmp in bucket_dir.rglob(f"*{TMP_SUFFIX}"):
                tmp.unlink()
                report["tmp_removed"] += 1
            objects_dir = bucket_dir / "objects"
            data_dir = bucket_dir / "objects-data"
            versions_dir = bucket_dir / "versions"
            bmeta_path = bucket_dir / "bucket.json"
            versioning_on = False
            if bmeta_path.exists():
                versioning_on = BucketMeta.from_dict(
                    _read_json(bmeta_path)).versioning_enabled

            live_pointers = {}  # key -> ObjectMeta
            if objects_dir.exists():
                for p in objects_dir.glob("*.json"):
                    m = ObjectMeta.from_dict(_read_json(p))
                    live_pointers[m.key] = m

            # 1) repair half-finished enable/disable migrations and find
            #    version ids still referenced by live state
            referenced_ids: set[str] = set()
            legacy_hashes: set[str] = set()
            if versioning_on:
                # An enable may have crashed after flipping bucket.json but
                # before creating the versions dir / migrating any object.
                versions_dir.mkdir(exist_ok=True)
                for key, pointer in list(live_pointers.items()):
                    if not pointer.version_id:
                        self._migrate_object_to_versioned(bucket_dir.name, key)
                        new_pointer = ObjectMeta.from_dict(_read_json(
                            objects_dir / f"{_key_hash(key)}.json"))
                        live_pointers[key] = new_pointer
                        if new_pointer.version_id:
                            referenced_ids.add(new_pointer.version_id)
                        report["migrations_completed"] += 1
            if versions_dir.exists():
                for index_path in versions_dir.glob("*/*.json"):
                    d = _read_json(index_path)
                    key = d["key"]
                    pointer = live_pointers.get(key)
                    if pointer is None:
                        # index for a deleted (or never-committed) object
                        continue
                    for rec in d["versions"]:
                        referenced_ids.add(rec["version_id"])
                    if pointer.version_id and pointer.version_id != d.get("current_version_id"):
                        # commit crashed between index rewrite and pointer
                        # rewrite: trust the index (data is already there)
                        current_id = d["current_version_id"]
                        cur_rec = next((r for r in d["versions"]
                                        if r["version_id"] == current_id), None)
                        if cur_rec is not None:
                            fixed = ObjectMeta(
                                key=key, size=cur_rec["size"], etag=cur_rec["etag"],
                                version=cur_rec["version"], version_id=current_id,
                                created_at=pointer.created_at)
                            _atomic_write_json(objects_dir / f"{_key_hash(key)}.json",
                                               fixed.to_dict())
                            live_pointers[key] = fixed
            for key, pointer in live_pointers.items():
                if not pointer.version_id:
                    legacy_hashes.add(_key_hash(key))

            # 2) orphan data files: anything not a referenced version id and
            #    not a live legacy object
            if data_dir.exists():
                for data_file in list(data_dir.rglob("*")):
                    if not data_file.is_file():
                        continue
                    name = data_file.name
                    if name in legacy_hashes:
                        continue
                    if name in referenced_ids:
                        continue
                    data_file.unlink()
                    report["orphan_data_removed"] += 1
                # sweep now-empty shard directories
                for shard in sorted(data_dir.glob("*"), reverse=True):
                    if shard.is_dir() and not any(shard.iterdir()):
                        shard.rmdir()

            # 3) orphan version indexes (no live pointer)
            if versions_dir.exists():
                live_index_names = {f"{_key_hash(k)}.json" for k in live_pointers}
                for index_path in list(versions_dir.glob("*/*.json")):
                    if index_path.name not in live_index_names:
                        index_path.unlink()
                        report["orphan_index_removed"] += 1
                for shard in sorted(versions_dir.glob("*"), reverse=True):
                    if shard.is_dir() and not any(shard.iterdir()):
                        shard.rmdir()

            # 4) orphan multipart part data
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
            self._versions_dir(meta.name).mkdir(exist_ok=True)
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
            # rmtree: versioned layouts keep empty two-char shard directories
            # even after every object is gone, so plain rmdir would fail.
            for sub in ("objects", "objects-data", "uploads", "uploads-data", "versions"):
                shutil.rmtree(bucket_dir / sub, ignore_errors=True)
            self._bucket_meta_path(name).unlink()
            bucket_dir.rmdir()

    # -- quota -----------------------------------------------------------------
    def _usage_bytes_locked(self) -> int:
        total = 0
        # Legacy/unversioned object data: one file per pointer that carries
        # no version id. Versioned pointers are accounted through their
        # version index below (current + retained history), so skip them
        # here to avoid double counting.
        for meta_path in (self.root / "buckets").glob("*/objects/*.json"):
            d = _read_json(meta_path)
            if not d.get("version_id"):
                total += int(d["size"])
        # Versioned buckets: bytes accounted from version records, not from
        # walking data files -- a crash can transiently leave orphan data
        # that must not count against quota.
        for index_path in (self.root / "buckets").glob("*/versions/*/*.json"):
            for rec in _read_json(index_path).get("versions", []):
                total += int(rec["size"])
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

    # -- versioning helpers (must be called with global + bucket lock) --------
    def _read_version_index(self, bucket: str, key: str):
        """Return (current_version_id, [VersionRecord oldest..newest]) or
        None if the key has no version index."""
        path = self._version_index_path(bucket, key)
        if not path.exists():
            return None
        d = _read_json(path)
        return d["current_version_id"], [VersionRecord.from_dict(r) for r in d["versions"]]

    def _write_version_index(self, bucket: str, key: str,
                             current_version_id: str, records: list) -> None:
        path = self._version_index_path(bucket, key)
        path.parent.mkdir(parents=True, exist_ok=True)
        payload = {"key": key, "current_version_id": current_version_id,
                   "versions": [r.to_dict() for r in records]}
        _atomic_write_json(path, payload)

    def _plan_eviction(self, bmeta: BucketMeta, records: list,
                       protected_id: str | None) -> tuple[list, list]:
        """Pure retention-policy evaluation over *previously committed*
        records (oldest..newest). Returns (survivors, evicted).

        ``protected_id`` is the version that is still "current" in the old
        state (may be None when evaluating a full history for an immediate
        cap change): it is never evicted. Both caps apply to the rest:

          * max_versions: keep at most that many historical versions.
          * max_version_bytes: total bytes of historical versions.

        Eviction walks oldest -> newest and removes an *oldest prefix*
        only: once the remaining history fits, it stops -- a large old
        version never causes newer versions to be evicted in its place.
        """
        max_n = bmeta.max_versions
        max_bytes = bmeta.max_version_bytes

        def violates(rs) -> bool:
            history = [r for r in rs if r.version_id != protected_id]
            return ((max_n and len(history) > max_n)
                    or (max_bytes and sum(r.size for r in history) > max_bytes))

        # Remove an oldest prefix of *non-protected* versions. Decision per
        # boundary: once removing version i makes the remainder fit, stop --
        # the prefix [0..i] is the eviction set even though some of those
        # versions would individually have fit (oldest-first is the point).
        survivors = list(records)
        evicted = []
        while violates(survivors):
            history = [r for r in survivors
                       if r.version_id != protected_id]
            if not history:
                break
            # oldest surviving *historical* version (records are ordered
            # oldest -> newest; the protected current may sit among them)
            oldest = history[0]
            survivors.remove(oldest)
            evicted.append(oldest)
        return survivors, evicted

    def _commit_versioned(self, bucket: str, bmeta: BucketMeta, key: str,
                          tmp_path: Path, size: int, etag: str, created_at: float,
                          last_version: int) -> ObjectMeta:
        """Promote a fully-written tmp file to a new current version.

        Commit order is crash-safe: quota is checked against the projected
        state (including bytes the retention policy will reclaim), data is
        renamed in first, evicted bytes are unlinked, then the version
        index and object pointer are rewritten atomically.
        """
        version_id = new_id()
        record = VersionRecord(version_id=version_id, version=last_version + 1,
                               size=size, etag=etag)
        existing = self._read_version_index(bucket, key)
        old_records = existing[1] if existing else []
        old_current_id = existing[0] if existing else None
        old_current = next((r for r in old_records if r.version_id == old_current_id), None)
        # Plan retention against the post-commit state: old records plus the
        # brand-new record, which is the next current and therefore the only
        # protected id. The superseded old current becomes ordinary history.
        projected = old_records + [record]
        projected_survivors, evicted = self._plan_eviction(
            bmeta, projected, version_id)
        old_survivors = [r for r in projected_survivors
                         if r.version_id != version_id]
        reclaimed = sum(r.size for r in evicted)
        delta = size - (old_current.size if old_current else 0) - reclaimed
        self._check_quota_locked(delta)
        data_path = self._version_data_path(bucket, version_id)
        data_path.parent.mkdir(parents=True, exist_ok=True)
        os.replace(tmp_path, data_path)  # new bytes reachable
        for rec in evicted:
            (self._version_data_path(bucket, rec.version_id)).unlink(missing_ok=True)
        survivors = old_survivors + [record]
        self._write_version_index(bucket, key, version_id, survivors)
        meta = ObjectMeta(key=key, size=size, etag=etag,
                          version=last_version + 1, version_id=version_id,
                          created_at=created_at)
        _atomic_write_json(self._objects_dir(bucket) / f"{_key_hash(key)}.json",
                           meta.to_dict())
        return meta

    def _migrate_object_to_versioned(self, bucket: str, key: str) -> None:
        """Best-effort idempotent migration of one legacy-layout object into
        the versioned layout. Skips keys that are already versioned or
        missing. Used when enabling versioning and during recovery of a
        half-finished enable."""
        pointer = self._objects_dir(bucket) / f"{_key_hash(key)}.json"
        if not pointer.exists():
            return
        meta = ObjectMeta.from_dict(_read_json(pointer))
        if meta.version_id:
            return  # already migrated
        if self._read_version_index(bucket, key) is not None:
            return  # index exists from an earlier partial run; pointer rewrite wins
        version_id = new_id()
        legacy_data = self._objects_data_dir(bucket) / _key_hash(key)
        if not legacy_data.exists():
            return
        target = self._version_data_path(bucket, version_id)
        target.parent.mkdir(parents=True, exist_ok=True)
        os.link(legacy_data, target)  # hard link: same bytes, new stable path
        record = VersionRecord(version_id=version_id, version=meta.version,
                               size=meta.size, etag=meta.etag,
                               created_at=meta.created_at)
        self._write_version_index(bucket, key, version_id, [record])
        meta.version_id = version_id
        _atomic_write_json(pointer, meta.to_dict())
        legacy_data.unlink(missing_ok=True)

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
            bmeta = self.get_bucket(bucket)
            if bmeta is None:
                raise NoSuchBucket(f"bucket '{bucket}' does not exist")
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
            if bmeta.versioning_enabled:
                # sharded layout: data lives at objects-data/<vid[:2]>/<vid>,
                # tmp alongside so the final promotion is a same-dir rename.
                tmp_path = (self._objects_data_dir(bucket) / "tmp"
                            / f"{new_id()}{TMP_SUFFIX}")
            else:
                data_path = self._objects_data_dir(bucket) / h
                tmp_path = data_path.with_name(data_path.name + TMP_SUFFIX)
            tmp_path.parent.mkdir(parents=True, exist_ok=True)
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
            if bmeta.versioning_enabled:
                # A legacy-layout object can survive an interrupted enable;
                # finish its migration before committing the next version.
                if existing is not None and not existing.version_id:
                    self._migrate_object_to_versioned(bucket, key)
                    existing = self.get_object_meta(bucket, key)
                idx = self._read_version_index(bucket, key)
                last_version = max(
                    (max((r.version for r in idx[1]), default=0) if idx else 0),
                    existing.version if existing else 0)
                created_at = existing.created_at if existing else time.time()
                try:
                    return self._commit_versioned(
                        bucket, bmeta, key, tmp_path, size, digest.hexdigest(),
                        created_at, last_version)
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

    def open_object_data(self, bucket: str, key: str,
                         version_id: str | None = None) -> tuple[ObjectMeta, BinaryIO]:
        """Resolve metadata and open the body file *atomically* under the
        bucket lock.

        The returned (meta, open file) are guaranteed to describe the same
        immutable version: even if another thread overwrites the object or
        evicts old versions while the caller is still streaming, the file
        descriptor stays valid on POSIX (unlinked bytes survive until the
        last fd closes) and the meta snapshot never changes. Caller must
        close the file.

        version_id=None reads the current version. An unknown/evicted id
        raises NoSuchVersion; a missing key raises NoSuchKey.
        """
        with self._global_lock, self._bucket_lock(bucket):
            if version_id is not None:
                index = self._read_version_index(bucket, key)
                if index is None:
                    raise NoSuchVersion(
                        f"version '{version_id}' of '{key}' does not exist "
                        f"in bucket '{bucket}'",
                        detail={"bucket": bucket, "key": key, "version_id": version_id})
                current_id, records = index
                rec = next((r for r in records if r.version_id == version_id), None)
                if rec is None:
                    raise NoSuchVersion(
                        f"version '{version_id}' of '{key}' does not exist or "
                        f"has been evicted in bucket '{bucket}'",
                        detail={"bucket": bucket, "key": key, "version_id": version_id})
                data_path = self._version_data_path(bucket, rec.version_id)
                f = open(data_path, "rb")  # pinned under the lock
                meta = ObjectMeta(key=key, size=rec.size, etag=rec.etag,
                                  version=rec.version,
                                  version_id=rec.version_id,
                                  created_at=rec.created_at, updated_at=rec.created_at)
                return meta, f
            meta = self.get_object_meta(bucket, key)
            if meta is None:
                raise NoSuchKey(f"object '{key}' does not exist in bucket '{bucket}'")
            if meta.version_id:
                data_path = self._version_data_path(bucket, meta.version_id)
            else:
                data_path = self._objects_data_dir(bucket) / _key_hash(key)
            f = open(data_path, "rb")  # pinned under the lock
            return meta, f

    def read_object_stream(self, bucket: str, key: str,
                           version_id: str | None = None) -> Iterator[bytes]:
        """Yield object bytes in bounded chunks; never loads it all. The
        meta/header resolution and body open happen in one locked step."""
        meta, f = self.open_object_data(bucket, key, version_id=version_id)
        chunk = self.settings.stream_chunk_size
        try:
            while True:
                buf = f.read(chunk)
                if not buf:
                    break
                yield buf
        finally:
            f.close()

    # -- version history -------------------------------------------------------
    def list_versions(self, bucket: str, key: str) -> tuple[str, list]:
        """Return (current_version_id, [VersionRecord newest..oldest]).
        Raises NoSuchKey if the object does not exist at all."""
        with self._global_lock, self._bucket_lock(bucket):
            if self.get_object_meta(bucket, key) is None:
                raise NoSuchKey(f"object '{key}' does not exist in bucket '{bucket}'")
            index = self._read_version_index(bucket, key)
            if index is None:
                return "", []
            current_id, records = index
            return current_id, list(reversed(records))

    def restore_version(self, bucket: str, key: str, version_id: str,
                        if_version: int | None = None,
                        if_absent: bool = False) -> ObjectMeta:
        """Re-commit a historical version as a brand-new current version.

        The source version's bytes are never mutated: they are copied into a
        fresh version record. The write obeys the same conditional rules as a
        normal overwrite (if_absent / if_version) and creates a new stable
        version id, so history is append-only and safe to repeat."""
        with self._global_lock, self._bucket_lock(bucket):
            bmeta = self.get_bucket(bucket)
            if bmeta is None:
                raise NoSuchBucket(f"bucket '{bucket}' does not exist")
            if not bmeta.versioning_enabled:
                from .errors import InvalidRequest
                raise InvalidRequest(
                    f"versioning is not enabled on bucket '{bucket}'")
            index = self._read_version_index(bucket, key)
            if index is None:
                raise NoSuchVersion(
                    f"version '{version_id}' of '{key}' does not exist",
                    detail={"bucket": bucket, "key": key, "version_id": version_id})
            _, records = index
            src = next((r for r in records if r.version_id == version_id), None)
            if src is None:
                raise NoSuchVersion(
                    f"version '{version_id}' of '{key}' does not exist or has "
                    f"been evicted",
                    detail={"bucket": bucket, "key": key, "version_id": version_id})
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
            src_path = self._version_data_path(bucket, src.version_id)
            tmp_path = (self._objects_data_dir(bucket) / "tmp"
                        / f"{new_id()}{TMP_SUFFIX}")
            tmp_path.parent.mkdir(parents=True, exist_ok=True)
            try:
                with open(src_path, "rb") as fin, open(tmp_path, "wb") as fout:
                    while True:
                        buf = fin.read(self.settings.stream_chunk_size)
                        if not buf:
                            break
                        fout.write(buf)
                    fout.flush()
                    os.fsync(fout.fileno())
            except BaseException:
                tmp_path.unlink(missing_ok=True)
                raise
            last_version = max((r.version for r in records),
                               default=existing.version if existing else 0)
            created_at = existing.created_at if existing else time.time()
            try:
                return self._commit_versioned(
                    bucket, bmeta, key, tmp_path, src.size, src.etag,
                    created_at, last_version)
            except BaseException:
                tmp_path.unlink(missing_ok=True)
                raise

    def set_versioning(self, name: str, enabled: bool,
                       max_versions: int | None = None,
                       max_version_bytes: int | None = None) -> BucketMeta:
        """Enable/disable history retention and update retention caps.

        Enabling migrates every existing legacy-layout object into the
        sharded versioned layout (its current content becomes version 1 of
        history); disabling drops all retained history for every key and
        migrates survivors back to the legacy layout, freeing space."""
        with self._global_lock, self._bucket_lock(name):
            bmeta = self.get_bucket(name)
            if bmeta is None:
                raise NoSuchBucket(f"bucket '{name}' does not exist")
            if max_versions is not None:
                if max_versions < 0:
                    from .errors import InvalidRequest
                    raise InvalidRequest("max_versions must be >= 0")
                bmeta.max_versions = max_versions
            if max_version_bytes is not None:
                if max_version_bytes < 0:
                    from .errors import InvalidRequest
                    raise InvalidRequest("max_version_bytes must be >= 0")
                bmeta.max_version_bytes = max_version_bytes
            if enabled and not bmeta.versioning_enabled:
                self._versions_dir(name).mkdir(exist_ok=True)
                bmeta.versioning_enabled = True
                # migrate current objects; idempotent if a previous run died
                for pointer in self._objects_dir(name).glob("*.json"):
                    meta = ObjectMeta.from_dict(_read_json(pointer))
                    self._migrate_object_to_versioned(name, meta.key)
            elif not enabled and bmeta.versioning_enabled:
                self._demigrate_bucket_locked(name)
                bmeta.versioning_enabled = False
            elif enabled and bmeta.versioning_enabled:
                # caps tightened/loosened without a state transition: apply
                # the new retention policy immediately.
                for pointer in self._objects_dir(name).glob("*.json"):
                    meta = ObjectMeta.from_dict(_read_json(pointer))
                    idx = self._read_version_index(name, meta.key)
                    if idx is None:
                        continue
                    survivors, evicted = self._plan_eviction(
                        bmeta, idx[1], idx[0])
                    for rec in evicted:
                        (self._version_data_path(name, rec.version_id)).unlink(missing_ok=True)
                    if survivors != idx[1]:
                        self._write_version_index(name, meta.key, idx[0], survivors)
            # persist enabled flag and/or cap changes in every case
            _atomic_write_json(self._bucket_meta_path(name), bmeta.to_dict())
            return bmeta

    def _demigrate_bucket_locked(self, name: str) -> None:
        """Move every versioned object back to the legacy flat layout and
        delete all non-current history (retention is being switched off)."""
        for pointer in self._objects_dir(name).glob("*.json"):
            meta = ObjectMeta.from_dict(_read_json(pointer))
            if not meta.version_id:
                continue
            idx = self._read_version_index(name, meta.key)
            if idx is None:
                continue
            current_id, records = idx
            current = next((r for r in records if r.version_id == current_id), None)
            flat = self._objects_data_dir(name) / _key_hash(meta.key)
            if current is not None:
                src = self._version_data_path(name, current.version_id)
                with open(src, "rb") as fin, open(flat, "wb") as fout:
                    shutil.copyfileobj(fin, fout, self.settings.stream_chunk_size)
                    fout.flush()
                    os.fsync(fout.fileno())
            for rec in records:
                (self._version_data_path(name, rec.version_id)).unlink(missing_ok=True)
            self._version_index_path(name, meta.key).unlink(missing_ok=True)
            legacy = ObjectMeta(key=meta.key, size=meta.size, etag=meta.etag,
                                version=meta.version, created_at=meta.created_at)
            _atomic_write_json(pointer, legacy.to_dict())

    def delete_object(self, bucket: str, key: str) -> ObjectMeta:
        with self._global_lock, self._bucket_lock(bucket):
            meta = self.get_object_meta(bucket, key)
            if meta is None:
                raise NoSuchKey(f"object '{key}' does not exist in bucket '{bucket}'")
            # metadata first: a crash leaves recoverable orphan data, never
            # metadata without a body.
            (self._objects_dir(bucket) / f"{_key_hash(key)}.json").unlink()
            if meta.version_id:
                index = self._read_version_index(bucket, key)
                if index is not None:
                    _, records = index
                    for rec in records:
                        (self._version_data_path(bucket, rec.version_id)).unlink(missing_ok=True)
                self._version_index_path(bucket, key).unlink(missing_ok=True)
            else:
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
            bmeta = self.get_bucket(bucket)
            if bmeta is None:
                raise NoSuchBucket(f"bucket '{bucket}' does not exist")
            if bmeta.versioning_enabled:
                tmp_path = (self._objects_data_dir(bucket) / "tmp"
                            / f"{new_id()}{TMP_SUFFIX}")
            else:
                data_path = self._objects_data_dir(bucket) / h
                tmp_path = data_path.with_name(data_path.name + TMP_SUFFIX)
            tmp_path.parent.mkdir(parents=True, exist_ok=True)
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
            if bmeta.versioning_enabled:
                if existing is not None and not existing.version_id:
                    self._migrate_object_to_versioned(bucket, key)
                    existing = self.get_object_meta(bucket, key)
                idx = self._read_version_index(bucket, key)
                last_version = max(
                    (max((r.version for r in idx[1]), default=0) if idx else 0),
                    existing.version if existing else 0)
                created_at = existing.created_at if existing else time.time()
                try:
                    meta = self._commit_versioned(
                        bucket, bmeta, key, tmp_path, total, digest.hexdigest(),
                        created_at, last_version)
                except BaseException:
                    tmp_path.unlink(missing_ok=True)
                    raise
            else:
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
