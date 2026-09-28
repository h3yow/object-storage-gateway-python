"""Filesystem-backed metadata + data store.

Layout under data_dir:

    buckets/<bucket>/bucket.json                 BucketMeta
    buckets/<bucket>/objects/<hash>.json         ObjectMeta   (committed last)
    buckets/<bucket>/objects-data/<hash>         object bytes (legacy, non-versioned)
    buckets/<bucket>/versions/<hash>/<vid>.json  VersionMeta (current + history)
    buckets/<bucket>/versions-data/<hash>/<vid>  version bytes (immutable once named)
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
import time
from pathlib import Path
from typing import Iterator

from .config import Settings
from .errors import (BucketAlreadyExists, BucketNotEmpty, EntityTooLarge,
                     InvalidPart, NoSuchBucket, NoSuchKey, NoSuchUpload,
                     NoSuchVersion, QuotaExceeded, TooManyRequests)
from .models import BucketMeta, ObjectMeta, PartMeta, UploadMeta, VersionMeta, new_id

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

    def _versions_dir(self, bucket: str, key: str) -> Path:
        return self._bucket_dir(bucket) / "versions" / _key_hash(key)

    def _versions_data_dir(self, bucket: str, key: str) -> Path:
        return self._bucket_dir(bucket) / "versions-data" / _key_hash(key)

    def _version_meta_path(self, bucket: str, key: str, version_id: str) -> Path:
        return self._versions_dir(bucket, key) / f"{version_id}.json"

    def _version_data_path(self, bucket: str, key: str, version_id: str) -> Path:
        return self._versions_data_dir(bucket, key) / version_id

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
            current_ids: dict[str, str] = {}
            if objects_dir.exists():
                for mp in objects_dir.glob("*.json"):
                    current_ids[mp.stem] = _read_json(mp).get("version_id", "")
            if data_dir.exists():
                for data_file in data_dir.iterdir():
                    # legacy data is live only while the current meta still
                    # points at it (empty version_id == legacy location).
                    if current_ids.get(data_file.name, None) != "":
                        data_file.unlink()
                        report["orphan_data_removed"] += 1
            self._recover_versions_locked(bucket_dir, current_ids, report)
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

    def _recover_versions_locked(self, bucket_dir: Path,
                                 current_ids: dict[str, str], report: dict) -> None:
        """Restore invariants in versions/ and versions-data/:

          * a version whose key has no current ObjectMeta is removed
            (the key was deleted and a crash interrupted cleanup);
          * a version data file without matching VersionMeta is removed;
          * a VersionMeta whose data file is missing is removed.
        """
        versions_root = bucket_dir / "versions"
        vdata_root = bucket_dir / "versions-data"
        live_meta: dict[Path, Path] = {}  # meta path -> data path
        if versions_root.exists():
            for vdir in versions_root.iterdir():
                if not vdir.is_dir():
                    continue
                h = vdir.name
                data_vdir = vdata_root / h if vdata_root.exists() else None
                if h not in current_ids:
                    for p in vdir.glob("*.json"):
                        p.unlink()
                        report["orphan_data_removed"] += 1
                    if data_vdir and data_vdir.exists():
                        for p in data_vdir.iterdir():
                            p.unlink()
                        report["orphan_data_removed"] += 1
                    vdir.rmdir()
                    if data_vdir and data_vdir.exists():
                        data_vdir.rmdir()
                    continue
                for mp in vdir.glob("*.json"):
                    dp = (data_vdir or (vdata_root / h)) / mp.stem
                    if dp.exists():
                        live_meta[mp] = dp
                    else:
                        mp.unlink()  # metadata without body must never survive
                        report["orphan_data_removed"] += 1
        if vdata_root.exists():
            for data_vdir in vdata_root.iterdir():
                h = data_vdir.name
                meta_vdir = versions_root / h
                live_data = {live_meta[p].name for p in live_meta if p.parent.name == h}
                for dp in data_vdir.iterdir():
                    if dp.name not in live_data:
                        dp.unlink()
                        report["orphan_data_removed"] += 1
                if not any(data_vdir.iterdir()):
                    data_vdir.rmdir()
                if meta_vdir.exists() and not any(meta_vdir.glob("*.json")):
                    meta_vdir.rmdir()

    # -- buckets -------------------------------------------------------------
    def create_bucket(self, meta: BucketMeta) -> None:
        with self._global_lock:
            path = self._bucket_meta_path(meta.name)
            if path.exists():
                raise BucketAlreadyExists(f"bucket '{meta.name}' already exists")
            self._bucket_dir(meta.name).mkdir(parents=True, exist_ok=True)
            self._objects_dir(meta.name).mkdir(exist_ok=True)
            self._objects_data_dir(meta.name).mkdir(exist_ok=True)
            (self._bucket_dir(meta.name) / "versions").mkdir(exist_ok=True)
            (self._bucket_dir(meta.name) / "versions-data").mkdir(exist_ok=True)
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

    def set_versioning_config(self, meta: BucketMeta, previous_enabled: bool) -> None:
        """Persist a new versioning configuration together with the on-disk
        effects of the transition, all under one bucket lock:

          * enabling (first time or re-enabling): existing legacy currents
            are promoted into version storage and gain a stable version id;
            the sticky ``versioning_ever_enabled`` latch is set;
          * disabling: every retained version is frozen so subsequent
            overwrites cannot remove it.
        """
        with self._global_lock, self._bucket_lock(meta.name):
            if not self._bucket_meta_path(meta.name).exists():
                raise NoSuchBucket(f"bucket '{meta.name}' does not exist")
            if meta.versioning_enabled and not previous_enabled:
                meta.versioning_ever_enabled = True
                self.enable_versioning_locked(meta.name)
            elif not meta.versioning_enabled and previous_enabled:
                self.disable_versioning_locked(meta.name)
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
            for sub in ("objects", "objects-data", "uploads", "uploads-data",
                        "versions", "versions-data"):
                sub_dir = bucket_dir / sub
                if sub in ("versions", "versions-data"):
                    if sub_dir.exists():
                        for d in sub_dir.iterdir():
                            for p in d.iterdir():
                                p.unlink()
                            d.rmdir()
                if sub_dir.exists():
                    sub_dir.rmdir()
            self._bucket_meta_path(name).unlink()
            bucket_dir.rmdir()

    # -- quota -----------------------------------------------------------------
    def _usage_bytes_locked(self) -> int:
        total = 0
        # Legacy (non-versioned) object bodies. Versioned current bodies are
        # counted once through their VersionMeta instead.
        for meta_path in (self.root / "buckets").glob("*/objects/*.json"):
            d = _read_json(meta_path)
            if not d.get("version_id"):
                total += int(d["size"])
        # Every kept version (current included) occupies exactly one data file.
        for vmeta_path in (self.root / "buckets").glob("*/versions/*/*.json"):
            total += int(_read_json(vmeta_path)["size"])
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

    # -- versioning -----------------------------------------------------------
    def _list_version_metas_locked(self, bucket: str, key: str) -> list[VersionMeta]:
        vdir = self._versions_dir(bucket, key)
        if not vdir.exists():
            return []
        metas = [VersionMeta.from_dict(_read_json(p)) for p in vdir.glob("*.json")]
        metas.sort(key=lambda v: (v.version, v.created_at))
        return metas

    def list_versions(self, bucket: str, key: str) -> list[VersionMeta]:
        with self._bucket_lock(bucket):
            return self._list_version_metas_locked(bucket, key)

    def get_version_snapshot(self, bucket: str, key: str) -> tuple[ObjectMeta | None, list[VersionMeta]]:
        """Atomically read the current ObjectMeta together with all stored
        versions of the key (oldest first), so the listing can never mix
        pre- and post-overwrite states."""
        with self._bucket_lock(bucket):
            current = self.get_object_meta(bucket, key)
            versions = self._list_version_metas_locked(bucket, key)
            return current, versions

    def _eviction_plan_locked(self, bucket: BucketMeta,
                              versions: list[VersionMeta],
                              current_vid: str) -> list[VersionMeta]:
        """Oldest non-current, non-frozen versions are evicted first. The
        current version and frozen versions (history retained from a period
        when versioning was disabled) are never part of the plan. Both
        limits apply independently."""
        history = [v for v in versions
                   if v.version_id != current_vid and not v.frozen]
        history.sort(key=lambda v: (v.version, v.created_at))
        keep_count = bucket.max_versions - 1 if bucket.max_versions else len(history)
        keep_bytes = bucket.max_version_bytes
        total = sum(v.size for v in history)
        evict: list[VersionMeta] = []
        for v in history:
            too_many = (len(history) - len(evict)) > keep_count if bucket.max_versions else False
            too_big = (total > keep_bytes) if keep_bytes else False
            if too_many or too_big:
                evict.append(v)
                total -= v.size
            else:
                break
        return evict

    def _remove_version_locked(self, bucket: str, key: str, v: VersionMeta) -> None:
        # metadata first: a crash leaves orphan data only, recoverable.
        self._version_meta_path(bucket, key, v.version_id).unlink(missing_ok=True)
        self._version_data_path(bucket, key, v.version_id).unlink(missing_ok=True)

    def _archive_current_locked(self, bucket: str, key: str,
                                existing: ObjectMeta) -> None:
        """Preserve the about-to-be-replaced current content as an immutable
        historical version. A versioned current already has its VersionMeta
        (written atomically when it became current), so only a pre-versioning
        legacy body needs migrating. Migration is idempotent across retries."""
        if existing.version_id:
            return  # already an immutable, retained version
        vid = new_id()
        self._versions_dir(bucket, key).mkdir(parents=True, exist_ok=True)
        self._versions_data_dir(bucket, key).mkdir(parents=True, exist_ok=True)
        target_data = self._version_data_path(bucket, key, vid)
        if not target_data.exists():
            src = self._objects_data_dir(bucket) / _key_hash(key)
            tmp = target_data.with_name(target_data.name + TMP_SUFFIX)
            with open(src, "rb") as fin, open(tmp, "wb") as fout:
                while True:
                    buf = fin.read(self.settings.stream_chunk_size)
                    if not buf:
                        break
                    fout.write(buf)
                fout.flush()
                os.fsync(fout.fileno())
            os.replace(tmp, target_data)
        vmeta = VersionMeta(key=key, version=existing.version, version_id=vid,
                            size=existing.size, etag=existing.etag,
                            created_at=existing.updated_at)
        _atomic_write_json(self._version_meta_path(bucket, key, vid), vmeta.to_dict())
        # migration complete; legacy body is now redundant (a crash before
        # this point simply leaves it for recover() to reap)
        (self._objects_data_dir(bucket) / _key_hash(key)).unlink(missing_ok=True)

    def _promote_legacy_current_locked(self, bucket: str, key: str,
                                       meta: ObjectMeta) -> ObjectMeta:
        """Give a legacy current (empty version_id, body in objects-data/) a
        stable version id by moving its body into version storage and
        recording a VersionMeta. Returns the updated ObjectMeta.

        Crash-safe and idempotent: a retry adopts a half-finished migration
        (a same-content VersionMeta left behind by a crash) instead of
        failing on the already-removed legacy source. A legacy current can
        only exist before versioning first touched the key, so any matching
        VersionMeta found here must be such a leftover."""
        if meta.version_id:
            return meta
        self._versions_dir(bucket, key).mkdir(parents=True, exist_ok=True)
        vdata_dir = self._versions_data_dir(bucket, key)
        vdata_dir.mkdir(parents=True, exist_ok=True)
        vid = ""
        for v in self._list_version_metas_locked(bucket, key):
            if (v.size == meta.size and v.etag == meta.etag
                    and self._version_data_path(bucket, key, v.version_id).exists()):
                vid = v.version_id
                break
        if not vid:
            vid = new_id()
            target_data = self._version_data_path(bucket, key, vid)
            tmp = target_data.with_name(target_data.name + TMP_SUFFIX)
            src = self._objects_data_dir(bucket) / _key_hash(key)
            with open(src, "rb") as fin, open(tmp, "wb") as fout:
                while True:
                    buf = fin.read(self.settings.stream_chunk_size)
                    if not buf:
                        break
                    fout.write(buf)
                fout.flush()
                os.fsync(fout.fileno())
            os.replace(tmp, target_data)
        vmeta = VersionMeta(key=key, version=meta.version, version_id=vid,
                            size=meta.size, etag=meta.etag,
                            created_at=meta.updated_at)
        _atomic_write_json(self._version_meta_path(bucket, key, vid), vmeta.to_dict())
        meta.version_id = vid
        _atomic_write_json(self._objects_dir(bucket) / f"{_key_hash(key)}.json",
                           meta.to_dict())
        (self._objects_data_dir(bucket) / _key_hash(key)).unlink(missing_ok=True)
        return meta

    def enable_versioning_locked(self, bucket: str) -> None:
        """Versioning was just switched on: every existing legacy current
        gets a stable, re-referenceable version id immediately, so listing
        and id-based reads work without waiting for the next overwrite.
        Caller holds the bucket lock."""
        for mp in self._objects_dir(bucket).glob("*.json"):
            meta = ObjectMeta.from_dict(_read_json(mp))
            if not meta.version_id:
                self._promote_legacy_current_locked(bucket, meta.key, meta)

    def disable_versioning_locked(self, bucket: str) -> None:
        """Versioning was just switched off: freeze every retained version so
        it survives subsequent overwrites (retention limits no longer apply
        while versioning is disabled). Caller holds the bucket lock."""
        versions_root = self._bucket_dir(bucket) / "versions"
        if not versions_root.exists():
            return
        for vpath in versions_root.glob("*/*.json"):
            v = VersionMeta.from_dict(_read_json(vpath))
            if not v.frozen:
                v.frozen = True
                _atomic_write_json(vpath, v.to_dict())

    def _commit_versioned_locked(self, bucket: str, bmeta: BucketMeta, key: str,
                                 existing: ObjectMeta | None,
                                 tmp_path: Path, size: int, etag: str) -> ObjectMeta:
        """Promote a fully-staged tmp body to a new current version, archive
        the previous current, enforce retention and quota. Caller holds the
        global + bucket locks and has validated write preconditions.

        Two modes, chosen by ``bmeta.versioning_enabled``:
          * enabled: the previous current becomes a retained *historical*
            version and the retention policy evicts old non-frozen versions;
          * disabled (in a bucket that once had versioning on): the previous
            current is *frozen* and kept indefinitely, no eviction runs, and
            the new body still gets a stable version id so the current
            version remains listable and readable by id.
        """
        enabled = bmeta.versioning_enabled
        versions = self._list_version_metas_locked(bucket, key)
        new_vid = new_id()
        new_version = (existing.version + 1) if existing else 1
        evict: list[VersionMeta] = []
        if enabled:
            evict = self._eviction_plan_locked(
                bmeta, versions + [VersionMeta(
                    key=key, version=new_version, version_id=new_vid,
                    size=size, etag=etag, created_at=0.0)],
                current_vid=new_vid)
        # quota: new body added, planned evictions release space; the old
        # current stays counted (it becomes a retained/frozen version).
        self._check_quota_locked(size - sum(v.size for v in evict))
        if existing is not None and existing.version_id:
            old_path = self._version_meta_path(bucket, key, existing.version_id)
            old_vmeta = VersionMeta.from_dict(_read_json(old_path))
            if not enabled and not old_vmeta.frozen:
                old_vmeta.frozen = True
                _atomic_write_json(old_path, old_vmeta.to_dict())
        # a legacy current (possible on the first versioned write) is migrated
        if existing is not None:
            self._archive_current_locked(bucket, key, existing)
        self._versions_dir(bucket, key).mkdir(parents=True, exist_ok=True)
        self._versions_data_dir(bucket, key).mkdir(parents=True, exist_ok=True)
        target_data = self._version_data_path(bucket, key, new_vid)
        os.replace(tmp_path, target_data)  # data committed first
        now = time.time()
        vmeta = VersionMeta(key=key, version=new_version, version_id=new_vid,
                            size=size, etag=etag, created_at=now)
        _atomic_write_json(self._version_meta_path(bucket, key, new_vid), vmeta.to_dict())
        meta = ObjectMeta(key=key, size=size, etag=etag,
                          version=new_version, version_id=new_vid,
                          created_at=existing.created_at if existing else now,
                          updated_at=now)
        _atomic_write_json(self._objects_dir(bucket) / f"{_key_hash(key)}.json",
                           meta.to_dict())
        for v in evict:
            self._remove_version_locked(bucket, key, v)
        return meta

    def _stream_into_tmp_locked(self, tmp_path: Path,
                                chunks: Iterator[bytes]) -> tuple[int, "hashlib._Hash"]:
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
        return size, digest

    def get_version_meta(self, bucket: str, key: str, version_id: str) -> VersionMeta | None:
        with self._bucket_lock(bucket):
            path = self._version_meta_path(bucket, key, version_id)
            if not path.exists():
                return None
            return VersionMeta.from_dict(_read_json(path))

    def open_version_data(self, bucket: str, key: str, version_id: str):
        """Open a specific version's body under the bucket lock. The returned
        file handle is independent of later overwrite/eviction renames and
        unlinks (POSIX keeps the inode alive for open handles), so header
        (size/etag) and body always come from the same version."""
        with self._bucket_lock(bucket):
            # Read directly: calling get_version_meta() here would re-enter
            # the global->bucket lock order while holding the bucket lock,
            # inverting the order writers use and deadlocking under load.
            path = self._version_meta_path(bucket, key, version_id)
            if not path.exists():
                return None, None
            vmeta = VersionMeta.from_dict(_read_json(path))
            f = open(self._version_data_path(bucket, key, version_id), "rb")  # noqa: SIM115
            return vmeta, f

    def promote_version(self, bucket: str, bmeta: BucketMeta, key: str,
                        version_id: str, if_version: int | None = None,
                        if_absent: bool = False) -> ObjectMeta:
        """Re-submit a historical version's content as a brand-new write.
        Preconditions follow the same rules as a normal overwrite; the source
        version is never modified (its body is hardlinked into the new
        version file)."""
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
            source = self.get_version_meta(bucket, key, version_id)
            if source is None:
                raise NoSuchVersion(
                    f"version '{version_id}' of '{key}' does not exist or was evicted")
            self._versions_data_dir(bucket, key).mkdir(parents=True, exist_ok=True)
            new_vid_tmp = self._versions_data_dir(bucket, key) / f".promote.{new_id()}{TMP_SUFFIX}"
            try:
                os.link(self._version_data_path(bucket, key, version_id), new_vid_tmp)
            except OSError:  # pragma: no cover - hardlinks assumed on local FS
                with open(self._version_data_path(bucket, key, version_id), "rb") as fin, \
                        open(new_vid_tmp, "wb") as fout:
                    while True:
                        buf = fin.read(self.settings.stream_chunk_size)
                        if not buf:
                            break
                        fout.write(buf)
                    fout.flush()
                    os.fsync(fout.fileno())
            # hardlink guarantees byte identity: reuse the source's hash/size
            try:
                return self._commit_versioned_locked(
                    bucket, bmeta, key, existing, new_vid_tmp,
                    source.size, source.etag)
            except BaseException:
                new_vid_tmp.unlink(missing_ok=True)
                raise

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
            if bmeta is not None and bmeta.versioning_ever_enabled:
                vdata_dir = self._versions_data_dir(bucket, key)
                vdata_dir.mkdir(parents=True, exist_ok=True)
                tmp_path = vdata_dir / f".staging.{new_id()}{TMP_SUFFIX}"
                size, digest = self._stream_into_tmp_locked(tmp_path, chunks)
                try:
                    return self._commit_versioned_locked(
                        bucket, bmeta, key, existing, tmp_path, size, digest.hexdigest())
                except BaseException:
                    tmp_path.unlink(missing_ok=True)
                    raise
            data_path = self._objects_data_dir(bucket) / h
            tmp_path = data_path.with_name(data_path.name + TMP_SUFFIX)
            size, digest = self._stream_into_tmp_locked(tmp_path, chunks)
            try:
                self._check_quota_locked(size - (existing.size if existing else 0))
            except BaseException:
                tmp_path.unlink(missing_ok=True)
                raise
            os.replace(tmp_path, data_path)  # data committed first
            meta = ObjectMeta(key=key, size=size, etag=digest.hexdigest(),
                              version=(existing.version + 1) if existing else 1)
            if existing:
                meta.created_at = existing.created_at
            _atomic_write_json(self._objects_dir(bucket) / f"{h}.json", meta.to_dict())
            return meta

    def open_current_object(self, bucket: str, key: str):
        """Resolve the current meta *and* open its body under one bucket lock.

        The returned header (size/etag/version_id) and file handle therefore
        always describe the same content. Streaming afterwards runs lock-free
        against a fixed inode: a racing overwrite (rename) or eviction/delete
        (unlink) can neither swap the body out from under the reader nor
        delete the data mid-read (POSIX keeps an unlinked/renamed inode alive
        for open handles). Returns (None, None) if the key does not exist."""
        with self._bucket_lock(bucket):
            meta = self.get_object_meta(bucket, key)
            if meta is None:
                return None, None
            if meta.version_id:
                f = open(self._version_data_path(bucket, key, meta.version_id), "rb")  # noqa: SIM115
                return meta, f
            f = open(self._objects_data_dir(bucket) / _key_hash(key), "rb")  # noqa: SIM115
            return meta, f

    def read_object_stream(self, bucket: str, key: str) -> tuple[ObjectMeta, Iterator[bytes]]:
        """Open the *current* object self-consistently and stream it in
        bounded chunks. See open_current_object for the consistency
        guarantee."""
        meta, f = self.open_current_object(bucket, key)
        if meta is None:
            raise NoSuchKey(f"object '{key}' does not exist in bucket '{bucket}'")
        chunk = self.settings.stream_chunk_size

        def _gen():
            try:
                while True:
                    buf = f.read(chunk)
                    if not buf:
                        break
                    yield buf
            finally:
                f.close()

        return meta, _gen()

    def read_version_stream(self, bucket: str, key: str,
                            version_id: str) -> tuple[VersionMeta, Iterator[bytes]]:
        """Stream one specific, immutable historical version. Returns
        NoSuchVersion if the id never existed or has since been evicted."""
        vmeta, f = self.open_version_data(bucket, key, version_id)
        if vmeta is None:
            raise NoSuchVersion(
                f"version '{version_id}' of '{key}' does not exist or was evicted")
        chunk = self.settings.stream_chunk_size

        def _gen():
            try:
                while True:
                    buf = f.read(chunk)
                    if not buf:
                        break
                    yield buf
            finally:
                f.close()

        return vmeta, _gen()

    def delete_object(self, bucket: str, key: str) -> ObjectMeta:
        with self._global_lock, self._bucket_lock(bucket):
            meta = self.get_object_meta(bucket, key)
            if meta is None:
                raise NoSuchKey(f"object '{key}' does not exist in bucket '{bucket}'")
            # current metadata first: a crash leaves recoverable orphan data,
            # never metadata without a body.
            (self._objects_dir(bucket) / f"{_key_hash(key)}.json").unlink()
            vdir = self._versions_dir(bucket, key)
            vddir = self._versions_data_dir(bucket, key)
            if vddir.exists():
                for p in vddir.iterdir():
                    p.unlink()
            if vdir.exists():
                # covers the frozen-history case (versioning was disabled and
                # the key later overwritten): metadata-less leftover bodies
                for p in vdir.glob("*.json"):
                    p.unlink()
            (self._objects_data_dir(bucket) / _key_hash(key)).unlink(missing_ok=True)
            vddir.rmdir() if vddir.exists() else None
            vdir.rmdir() if vdir.exists() else None
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
            bmeta = self.get_bucket(bucket)
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
            versioned = bmeta is not None and bmeta.versioning_ever_enabled
            if versioned:
                vdata_dir = self._versions_data_dir(bucket, key)
                vdata_dir.mkdir(parents=True, exist_ok=True)
                tmp_path = vdata_dir / f".staging.{new_id()}{TMP_SUFFIX}"
            else:
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
                if versioned:
                    meta = self._commit_versioned_locked(
                        bucket, bmeta, key, existing, tmp_path, total, digest.hexdigest())
                else:
                    self._check_quota_locked(total - (existing.size if existing else 0))
                    os.replace(tmp_path, data_path)
                    meta = ObjectMeta(key=key, size=total, etag=digest.hexdigest(),
                                      version=(existing.version + 1) if existing else 1)
                    if existing:
                        meta.created_at = existing.created_at
                    _atomic_write_json(self._objects_dir(bucket) / f"{h}.json",
                                       meta.to_dict())
            except BaseException:
                tmp_path.unlink(missing_ok=True)
                raise
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
