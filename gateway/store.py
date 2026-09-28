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
serialises concurrent writes to the same object. Reads take the bucket lock
only for the "resolve metadata + open data file" snapshot; the returned file
handle pins a fixed inode and the rest of the streaming runs lock-free, so a
racing overwrite/eviction/delete cannot make a read's header and body
disagree, even for legacy (in-place replaced) bodies.
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

    def snapshot_current_and_versions(
            self, bucket: str, key: str) -> tuple[ObjectMeta | None, list[VersionMeta]]:
        """Read the current ObjectMeta and the full version list in one
        bucket-lock critical section, so a listing can never mix the current
        pointer of one overwrite with the versions of another."""
        h = _key_hash(key)
        with self._bucket_lock(bucket):
            meta_path = self._objects_dir(bucket) / f"{h}.json"
            current = (ObjectMeta.from_dict(_read_json(meta_path))
                       if meta_path.exists() else None)
            versions = self._list_version_metas_locked(bucket, key)
            return current, versions

    def _eviction_plan_locked(self, bucket: BucketMeta,
                              versions: list[VersionMeta],
                              current_vid: str) -> list[VersionMeta]:
        """Oldest non-current versions are evicted first. The current version
        is never part of the plan. Both limits apply independently."""
        history = [v for v in versions if v.version_id != current_vid]
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

    def _freeze_legacy_body_locked(self, bucket: str, key: str,
                                    existing: ObjectMeta) -> str:
        """Copy a pre-versioning legacy body into immutable version storage
        and write its VersionMeta. Returns the assigned version id.

        Idempotent across retries/crashes: if an earlier attempt already
        froze this exact content (same size/etag/version), that version id is
        reused instead of producing a duplicate. The legacy body is removed
        only after the frozen copy is fully committed."""
        for v in self._list_version_metas_locked(bucket, key):
            if (v.version == existing.version and v.size == existing.size
                    and v.etag == existing.etag):
                # a previous attempt already froze this exact content: reuse it
                (self._objects_data_dir(bucket) / _key_hash(key)).unlink(missing_ok=True)
                return v.version_id
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
        return vid

    def _archive_current_locked(self, bucket: str, key: str,
                                existing: ObjectMeta) -> None:
        """Preserve the about-to-be-replaced current content as an immutable
        historical version. A versioned current already has its VersionMeta
        (written atomically when it became current), so only a pre-versioning
        legacy body needs migrating. Migration is idempotent across retries."""
        if existing.version_id:
            return  # already an immutable, retained version
        self._freeze_legacy_body_locked(bucket, key, existing)

    def _migrate_key_on_enable_locked(self, bucket: str, key: str) -> None:
        """Give a key's legacy current content a stable version id at the
        moment retention is switched on, and repoint the current ObjectMeta
        at it. No-op for keys that already live in version storage."""
        h = _key_hash(key)
        meta_path = self._objects_dir(bucket) / f"{h}.json"
        if not meta_path.exists():
            return
        existing = ObjectMeta.from_dict(_read_json(meta_path))
        if existing.version_id:
            return
        vid = self._freeze_legacy_body_locked(bucket, key, existing)
        existing.version_id = vid
        _atomic_write_json(meta_path, existing.to_dict())

    def _migrate_all_keys_on_enable_locked(self, bucket: str) -> None:
        for meta_path in self._objects_dir(bucket).glob("*.json"):
            self._migrate_key_on_enable_locked(bucket,
                                               ObjectMeta.from_dict(_read_json(meta_path)).key)

    def set_versioning(self, bmeta: BucketMeta, enabling: bool) -> None:
        """Persist bucket versioning settings. When retention is switched on
        for the first time, every existing (legacy) current object is migrated
        into version storage *before* the bucket flag is committed, so once
        enabled is visible, listing every key already returns a stable,
        re-referenceable id for its current version. Idempotent on retry."""
        with self._global_lock, self._bucket_lock(bmeta.name):
            if not self._bucket_meta_path(bmeta.name).exists():
                raise NoSuchBucket(f"bucket '{bmeta.name}' does not exist")
            if enabling:
                bmeta.versioning_enabled = True
                bmeta.versioned_ever = True
                self._migrate_all_keys_on_enable_locked(bmeta.name)
            _atomic_write_json(self._bucket_meta_path(bmeta.name), bmeta.to_dict())

    def _commit_versioned_locked(self, bucket: str, bmeta: BucketMeta, key: str,
                                 existing: ObjectMeta | None,
                                 tmp_path: Path, size: int, etag: str,
                                 preserve_history: bool = True) -> ObjectMeta:
        """Promote a fully-staged tmp body to a new current version, archive
        the previous current, enforce retention and quota. Caller holds the
        global + bucket locks and has validated write preconditions.

        ``preserve_history=False`` is used for an overwrite after retention
        has been switched off in a bucket that had it on: the new current is
        still committed to version storage with a stable id (so it stays
        listable and readable by id), but the replaced current is dropped
        rather than frozen, and no retention eviction runs -- the surviving
        history is the already-frozen history only."""
        versions = self._list_version_metas_locked(bucket, key)
        new_vid = new_id()
        new_version = (existing.version + 1) if existing else 1
        candidate = VersionMeta(
            key=key, version=new_version, version_id=new_vid,
            size=size, etag=etag, created_at=0.0)
        evict: list[VersionMeta] = []
        if preserve_history:
            evict = self._eviction_plan_locked(bmeta, versions + [candidate],
                                               current_vid=new_vid)
        # quota: new body added, planned evictions release space. When
        # history is not preserved the replaced current's body is released
        # too, and older frozen history stays accounted.
        released = sum(v.size for v in evict)
        if not preserve_history and existing is not None:
            released += existing.size
        self._check_quota_locked(size - released)
        if existing is not None:
            if preserve_history:
                self._archive_current_locked(bucket, key, existing)
            elif existing.version_id:
                # drop the replaced current; never the current pointer being
                # replaced in this very commit (the new ObjectMeta commit
                # below is what makes new_vid current)
                self._remove_version_locked(bucket, key, VersionMeta(
                    key=key, version=existing.version,
                    version_id=existing.version_id,
                    size=existing.size, etag=existing.etag,
                    created_at=existing.updated_at))
            else:  # defensive: legacy current in a formerly-versioned bucket
                (self._objects_data_dir(bucket) / _key_hash(key)).unlink(missing_ok=True)
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
            return self._open_version_data_locked(bucket, key, version_id)

    def _open_version_data_locked(self, bucket: str, key: str, version_id: str):
        # Caller holds the bucket lock. Read directly: re-entering via
        # open_version_data() is harmless with an RLock, but keeping this
        # helper lets key-existence checks share the same snapshot.
        path = self._version_meta_path(bucket, key, version_id)
        if not path.exists():
            return None, None
        vmeta = VersionMeta.from_dict(_read_json(path))
        f = open(self._version_data_path(bucket, key, version_id), "rb")  # noqa: SIM115
        return vmeta, f

    def open_version_for_read(self, bucket: str, key: str, version_id: str):
        """Single bucket-lock snapshot for a by-id read.

        Returns ``(current_exists, VersionMeta | None, file | None)``.
        ``current_exists`` answers whether the key currently exists; the meta
        and handle describe the requested immutable version. Both facts are
        observed atomically, so a delete racing the read cannot turn a
        missing key into a NoSuchVersion or a missing version into a
        NoSuchKey."""
        h = _key_hash(key)
        with self._bucket_lock(bucket):
            current_exists = (self._objects_dir(bucket) / f"{h}.json").exists()
            vmeta, f = self._open_version_data_locked(bucket, key, version_id)
            return current_exists, vmeta, f

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
            # A bucket that once had retention keeps committing the current
            # version through version storage (stable id, listable current);
            # only newly-preserved history follows the live enabled flag.
            if bmeta is not None and (bmeta.versioning_enabled or bmeta.versioned_ever):
                vdata_dir = self._versions_data_dir(bucket, key)
                vdata_dir.mkdir(parents=True, exist_ok=True)
                tmp_path = vdata_dir / f".staging.{new_id()}{TMP_SUFFIX}"
                size, digest = self._stream_into_tmp_locked(tmp_path, chunks)
                try:
                    return self._commit_versioned_locked(
                        bucket, bmeta, key, existing, tmp_path, size,
                        digest.hexdigest(), preserve_history=bmeta.versioning_enabled)
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

    def open_current_read(self, bucket: str, key: str):
        """Resolve the *current* content and open its body in one bucket-lock
        critical section.

        Returns ``(ObjectMeta | None, file | None)``: None for the meta when
        the key does not exist. The returned handle is an independent open of
        a fixed inode -- a concurrent overwrite (rename) or eviction/delete
        (unlink) cannot change the bytes the handle reads (POSIX keeps the
        inode alive for open handles). Streaming afterwards runs lock-free.
        This guarantees that the header (size/etag/version_id) and the body
        describe one and the same content, even for legacy bodies whose
        overwrite is an in-place replacement."""
        h = _key_hash(key)
        with self._bucket_lock(bucket):
            meta_path = self._objects_dir(bucket) / f"{h}.json"
            if not meta_path.exists():
                return None, None
            meta = ObjectMeta.from_dict(_read_json(meta_path))
            try:
                if meta.version_id:
                    path = self._version_data_path(bucket, key, meta.version_id)
                    f = open(path, "rb")  # noqa: SIM115
                else:
                    f = open(self._objects_data_dir(bucket) / h, "rb")  # noqa: SIM115
            except FileNotFoundError:
                # current meta without body: only possible right after a
                # crash before recover(); report cleanly rather than 500
                return None, None
            return meta, f

    def read_object_stream(self, bucket: str, key: str) -> Iterator[bytes]:
        """Yield the *current* object's bytes in bounded chunks.

        Thin streaming wrapper around :meth:`open_current_read`: the current
        version is located and its file opened under the bucket lock, so an
        overwrite/delete racing with the read cannot change the open file;
        length/etag header and body are guaranteed to describe the same
        version."""
        meta, f = self.open_current_read(bucket, key)
        if meta is None:
            raise NoSuchKey(f"object '{key}' does not exist in bucket '{bucket}'")
        chunk = self.settings.stream_chunk_size
        try:
            while True:
                buf = f.read(chunk)
                if not buf:
                    break
                yield buf
        finally:
            f.close()

    def read_version_stream(self, bucket: str, key: str,
                            version_id: str) -> tuple[VersionMeta, Iterator[bytes]]:
        """Stream one specific, immutable historical version. Key existence
        and version existence are resolved in one bucket-lock snapshot.

        Raises NoSuchKey if the key does not exist at the snapshot, otherwise
        NoSuchVersion when the id never existed or has since been evicted."""
        current_exists, vmeta, f = self.open_version_for_read(bucket, key, version_id)
        if not current_exists:
            if f is not None:
                f.close()
            raise NoSuchKey(f"object '{key}' does not exist in bucket '{bucket}'")
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
            versioned = bmeta is not None and (bmeta.versioning_enabled
                                               or bmeta.versioned_ever)
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
                        bucket, bmeta, key, existing, tmp_path, total,
                        digest.hexdigest(), preserve_history=bmeta.versioning_enabled)
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
