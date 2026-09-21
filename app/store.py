"""Core object-storage gateway.

Concurrency model
-----------------
* One ``asyncio.Lock`` per bucket (serializes bucket-level metadata changes
  and write-quota accounting) and one per (bucket, key) (serializes object
  state transitions). Locks are per-process; persistence is crash-safe via
  atomic rename + fsync (see metastore).
* A bounded per-bucket semaphore caps concurrent mutating requests; overflow
  is rejected with QUOTA_EXCEEDED *before* any temp state is created.

Visibility model
----------------
An object's payload is fully written/fsync'ed to a versioned blob before its
metadata is atomically published; readers only see complete versions. Old
blobs are deleted after the metadata commit.
"""
from __future__ import annotations

import asyncio
import logging
import time
import uuid
from pathlib import Path

from app import auth as auth_mod
from app import blobstore
from app.auth import Principal
from app.config import Settings
from app.errors import ErrorCode, GatewayError, error
from app.layout import Layout
from app.metastore import (
    async_remove_dir,
    async_remove_file,
    async_write_json,
    atomic_write_json,
    read_json,
    remove_dir,
    remove_file,
)
from app.models import (
    BucketPolicy,
    CompleteUploadResult,
    InitiateUploadResult,
    ListObjectsResult,
    ListPartsResult,
    ObjectMeta,
)

log = logging.getLogger("osg.store")

# Bucket quota fields (bytes / object count). 0 means unlimited.
DEFAULT_BUCKET_MAX_BYTES = 0
DEFAULT_BUCKET_MAX_OBJECTS = 0


class GatewayStore:
    def __init__(self, settings: Settings) -> None:
        self.settings = settings
        self.layout = Layout(settings)
        self._bucket_locks: dict[str, asyncio.Lock] = {}
        self._key_locks: dict[tuple[str, str], asyncio.Lock] = {}
        self._write_counts: dict[str, int] = {}
        self._locks_guard = asyncio.Lock()

    # ============ startup / recovery ============

    async def start(self) -> dict:
        self.layout.init_dirs()
        return await self.recover()

    async def recover(self) -> dict:
        """Reconcile disk state after (re)start.

        * bucket records referencing nothing and vice versa are reconciled;
        * stale temp/incoming files anywhere are removed;
        * unfinished multipart uploads are aborted (their parts deleted);
        * object blobs without metadata and metadata without blobs are
          reconciled so neither dangling metadata nor unreferenced payloads
          survive.
        """
        report: dict = {"buckets": 0, "aborted_uploads": 0,
                        "orphan_blobs": 0, "stray_tmp": 0, "orphan_meta": 0}
        self.layout.init_dirs()

        # 1. remove every temp/incoming file (crash mid-write)
        for tmp in self.layout.root.rglob(".*.tmp"):
            remove_file(tmp)
            report["stray_tmp"] += 1
        for incoming in self.layout.root.rglob(".incoming-*.part"):
            remove_file(incoming)
            report["stray_tmp"] += 1

        bucket_files = {p.name: p for p in self.layout.all_bucket_files()}
        report["buckets"] = len(bucket_files)

        # 2. abort unfinished uploads
        if self.settings.abort_orphan_uploads and self.layout.uploads_dir.exists():
            for bdir in self.layout.uploads_dir.iterdir():
                if not bdir.is_dir():
                    continue
                for kdir in bdir.rglob("*"):
                    if kdir.is_dir() and (kdir / "meta.json").exists():
                        remove_dir(kdir)
                        report["aborted_uploads"] += 1

        # 3. reconcile each bucket's objects and recompute its stats
        for bf in bucket_files.values():
            try:
                rec = read_json(bf)
            except Exception:  # corrupt record: leave for manual triage
                log.exception("corrupt bucket record %s", bf)
                continue
            bucket = rec["name"]
            await self._reconcile_bucket(bucket, rec, report)
        return report

    async def _reconcile_bucket(self, bucket: str, rec: dict, report: dict) -> None:
        # index metadata-declared versions
        objects: dict[str, dict] = rec.get("objects", {})
        blobs_root = self.layout.bucket_object_dir(bucket)
        on_disk: set[Path] = set()
        if blobs_root.exists():
            on_disk = {p for p in blobs_root.rglob("*.part") if p.is_file()}

        referenced: set[Path] = set()
        for key, meta in list(objects.items()):
            blob = self.layout.version_blob(bucket, key, meta["version_id"])
            if not blob.exists():
                # metadata without payload -> drop it
                del objects[key]
                report["orphan_meta"] += 1
                log.warning("recover: metadata without payload dropped: %s/%s",
                            bucket, key)
            else:
                referenced.add(blob)

        for orphan in on_disk - referenced:
            remove_file(orphan)
            report["orphan_blobs"] += 1
            log.warning("recover: orphan blob removed: %s", orphan)

        # prune empty key dirs
        if blobs_root.exists():
            for kd in sorted((p for p in blobs_root.iterdir() if p.is_dir()),
                             reverse=True):
                if not any(kd.iterdir()):
                    kd.rmdir()

        rec["objects"] = objects
        rec["stats"] = self._compute_stats(objects)
        rec["active_uploads"] = {}
        rec.setdefault("completed_uploads", {})
        # keep completed-upload idempotency markers bounded
        rec["completed_uploads"] = dict(
            list(rec["completed_uploads"].items())[-1000:]
        )
        self._save_bucket_record_sync(rec)

    @staticmethod
    def _compute_stats(objects: dict) -> dict:
        return {
            "object_count": len(objects),
            "bytes": sum(int(m.get("size", 0)) for m in objects.values()),
        }

    # ============ locking ============

    async def _get_lock(self, table: dict, key) -> asyncio.Lock:
        async with self._locks_guard:
            lock = table.get(key)
            if lock is None:
                lock = asyncio.Lock()
                table[key] = lock
            return lock

    async def _bucket_lock(self, bucket: str) -> asyncio.Lock:
        return await self._get_lock(self._bucket_locks, bucket)

    async def _key_lock(self, bucket: str, key: str) -> asyncio.Lock:
        return await self._get_lock(self._key_locks, (bucket, key))

    # ============ bucket records ============

    def _load_bucket_record(self, bucket: str) -> dict:
        path = self.layout.bucket_file(bucket)
        if not path.exists():
            raise error(ErrorCode.BUCKET_NOT_FOUND, f"bucket {bucket!r} does not exist")
        rec = read_json(path)
        rec.setdefault("objects", {})
        rec.setdefault("active_uploads", {})
        rec.setdefault("completed_uploads", {})
        rec.setdefault("stats", {"object_count": 0, "bytes": 0})
        rec.setdefault("quotas", {"max_bytes": DEFAULT_BUCKET_MAX_BYTES,
                                  "max_objects": DEFAULT_BUCKET_MAX_OBJECTS})
        return rec

    def _save_bucket_record_sync(self, rec: dict) -> None:
        rec["stats"] = self._compute_stats(rec["objects"])
        atomic_write_json(self.layout.bucket_file(rec["name"]), rec)

    async def _save_bucket_record(self, rec: dict) -> None:
        rec["stats"] = self._compute_stats(rec["objects"])
        await async_write_json(self.layout.bucket_file(rec["name"]), rec)

    def _policy_of(self, rec: dict) -> BucketPolicy:
        return BucketPolicy.model_validate(rec.get("policy") or {"statements": []})

    def _check_bucket_action(self, rec: dict, principal: Principal, action: str) -> None:
        policy = self._policy_of(rec)
        auth_mod.authorize(policy, principal, action, rec.get("owner"))

    # ============ buckets ============

    async def create_bucket(self, bucket: str, principal: Principal,
                            policy: BucketPolicy | None = None,
                            max_bytes: int = 0,
                            max_objects: int = 0) -> dict:
        self.layout.validate_bucket_name(bucket)
        path = self.layout.bucket_file(bucket)
        lock = await self._bucket_lock(bucket)
        async with lock:
            if path.exists():
                raise error(ErrorCode.BUCKET_EXISTS,
                            f"bucket {bucket!r} already exists")
            rec = {
                "name": bucket,
                "owner": principal.name,
                "created_at": time.time(),
                "policy": policy.model_dump() if policy else {"statements": []},
                "objects": {},
                "active_uploads": {},
                "completed_uploads": {},
                "stats": {"object_count": 0, "bytes": 0},
                "quotas": {"max_bytes": int(max_bytes),
                           "max_objects": int(max_objects)},
            }
            await self._save_bucket_record(rec)
            log.info("create_bucket bucket=%s by=%s (owner=%s)",
                     bucket, principal.name, principal.name)
            return {"name": bucket, "owner": principal.name}

    async def delete_bucket(self, bucket: str, principal: Principal) -> None:
        lock = await self._bucket_lock(bucket)
        async with lock:
            rec = self._load_bucket_record(bucket)
            self._check_bucket_action(rec, principal, "s3:DeleteBucket")
            if rec["objects"] or rec["active_uploads"]:
                raise error(
                    ErrorCode.BUCKET_NOT_EMPTY,
                    f"bucket {bucket!r} not empty "
                    f"({len(rec['objects'])} objects, "
                    f"{len(rec['active_uploads'])} unfinished uploads)",
                )
            # metadata first, then empty payload dirs
            await async_remove_file(self.layout.bucket_file(bucket))
            bod = self.layout.bucket_object_dir(bucket)
            if bod.exists():
                await async_remove_dir(bod)
            bud = self.layout.bucket_upload_dir(bucket)
            if bud.exists():
                await async_remove_dir(bud)
            log.info("delete_bucket bucket=%s by=%s", bucket, principal.name)

    async def list_buckets(self, principal: Principal) -> list[dict]:
        out = []
        for bf in self.layout.all_bucket_files():
            try:
                rec = read_json(bf)
            except (OSError, ValueError, GatewayError) as exc:
                # corrupt/unreadable record: skip, do not fail the whole listing
                log.warning("skipping unreadable bucket record %s: %s", bf, exc)
                continue
            if rec.get("owner") == principal.name:
                out.append({"name": rec["name"],
                            "created_at": rec.get("created_at"),
                            "stats": rec.get("stats", {})})
        return out

    # ============ objects ============

    async def _enter_write_gate(self, bucket: str) -> None:
        """Capacity control for concurrent mutating requests.

        Fail fast (QUOTA_EXCEEDED) instead of queueing, and do it *before* any
        temp state exists, so a rejected request leaves nothing behind.
        """
        async with self._locks_guard:
            current = self._write_counts.get(bucket, 0)
            if current >= self.settings.max_inflight_writes:
                raise error(
                    ErrorCode.QUOTA_EXCEEDED,
                    f"bucket {bucket!r} reached the concurrent-write limit of "
                    f"{self.settings.max_inflight_writes}; retry later",
                )
            self._write_counts[bucket] = current + 1

    async def _leave_write_gate(self, bucket: str) -> None:
        async with self._locks_guard:
            current = self._write_counts.get(bucket, 0)
            self._write_counts[bucket] = max(0, current - 1)

    async def list_objects(self, bucket, principal, prefix="", delimiter=None,
                           marker=None, max_keys=0) -> ListObjectsResult:
        self.layout.validate_bucket_name(bucket)
        rec = self._load_bucket_record(bucket)
        # Authz first: denied principals cannot enumerate to learn keys.
        self._check_bucket_action(rec, principal, "s3:ListBucket")

        limit = max_keys if 0 < max_keys <= self.settings.max_list_keys \
            else self.settings.max_list_keys
        keys = sorted(k for k in rec["objects"] if k.startswith(prefix or ""))
        if marker:
            # continuation marker: include the marker key itself on next page
            keys = [k for k in keys if k >= marker]

        objects: list[ObjectMeta] = []
        common: list[str] = []
        common_seen: set[str] = set()
        next_marker = None
        emitted = 0
        for key in keys:
            rel = key[len(prefix or ""):]
            if delimiter and delimiter in rel:
                cp = (prefix or "") + rel.split(delimiter, 1)[0] + delimiter
                if cp not in common_seen:
                    if emitted >= limit:
                        next_marker = key
                        break
                    common_seen.add(cp)
                    common.append(cp)
                    emitted += 1
                continue
            if emitted >= limit:
                next_marker = key
                break
            objects.append(ObjectMeta.model_validate(rec["objects"][key]))
            emitted += 1
        return ListObjectsResult(
            bucket=bucket, prefix=prefix or "", delimiter=delimiter,
            next_marker=next_marker, truncated=next_marker is not None,
            objects=objects, common_prefixes=common,
        )

    async def put_object(self, bucket, key, principal, chunks, if_match=None,
                         if_none_match=None, content_type=None) -> ObjectMeta:
        self.layout.validate_key(key)
        key_lock = await self._key_lock(bucket, key)
        async with key_lock:
            rec = self._load_bucket_record(bucket)  # 404 before authz policy lookup
            self._check_bucket_action(rec, principal, "s3:PutObject")

            existing = rec["objects"].get(key)
            # conditional write semantics evaluated before any bytes arrive
            if if_none_match == "*" and existing is not None:
                raise error(ErrorCode.PRECONDITION_FAILED,
                            f"If-None-Match: key {key!r} already exists")
            if if_match is not None and (existing is None or
                                         existing["etag"] != if_match.strip('"')):
                raise error(ErrorCode.PRECONDITION_FAILED,
                            "If-Match etag does not match current version")

            await self._enter_write_gate(bucket)
            try:
                obj_dir = self.layout.object_dir(bucket, key)
                tmp, size, digest = await blobstore.stream_sink(
                    obj_dir, chunks,
                    max_bytes=self.settings.max_object_size,
                    chunk_size=self.settings.stream_chunk_size,
                )
                try:
                    meta = await self._publish_version(
                        bucket, key, rec, tmp, size, digest,
                        content_type=content_type,
                    )
                except BaseException:
                    await async_remove_file(tmp)
                    raise
            finally:
                await self._leave_write_gate(bucket)
        log.info("put_object bucket=%s key=%r size=%d etag=%s by=%s",
                 bucket, key, size, digest[:12], principal.name)
        return meta

    async def _publish_version(self, bucket, key, rec, tmp_blob: Path,
                               size: int, etag: str,
                               content_type: str | None) -> ObjectMeta:
        """Atomically publish a fully-written blob as the current version.

        Bucket lock serializes the record read-modify-write across keys;
        the caller already holds the per-key lock for same-key serialization.
        """
        bucket_lock = await self._bucket_lock(bucket)
        async with bucket_lock:
            fresh = self._load_bucket_record(bucket)
            quota = fresh.get("quotas", {})
            existing = fresh["objects"].get(key)
            max_objects = int(quota.get("max_objects", 0) or 0)
            if max_objects and existing is None and \
                    len(fresh["objects"]) >= max_objects:
                raise error(ErrorCode.QUOTA_EXCEEDED,
                            f"bucket object-count quota {max_objects} reached")
            max_bytes = int(quota.get("max_bytes", 0) or 0)
            projected = fresh["stats"]["bytes"] - (existing["size"] if existing else 0) + size
            if max_bytes and projected > max_bytes:
                raise error(ErrorCode.QUOTA_EXCEEDED,
                            f"bucket byte quota {max_bytes} would be exceeded "
                            f"(projected {projected})")

            now = time.time()
            version_id = uuid.uuid4().hex
            dest = self.layout.version_blob(bucket, key, version_id)
            # same-directory rename: atomic, payload already fsync'ed
            import os
            await asyncio.to_thread(os.replace, tmp_blob, dest)
            old_blob = None
            if existing is not None:
                old_blob = self.layout.version_blob(
                    bucket, key, existing["version_id"])
            meta = ObjectMeta(
                bucket=bucket, key=key, size=size, etag=etag,
                version_id=version_id,
                created_at=existing["created_at"] if existing else now,
                updated_at=now, content_type=content_type,
            )
            fresh["objects"][key] = meta.model_dump()
            await self._save_bucket_record(fresh)   # commit point (durable rename)
        # metadata is committed: retire the previous version outside the lock
        if old_blob is not None:
            await async_remove_file(old_blob)
        return meta

    async def get_object(self, bucket, key, principal):
        self.layout.validate_key(key)
        rec = self._load_bucket_record(bucket)
        # Authz BEFORE existence: missing vs present keys look identical when denied.
        self._check_bucket_action(rec, principal, "s3:GetObject")
        meta = self._meta_or_404(rec, bucket, key)
        path = self.layout.version_blob(bucket, key, meta.version_id)
        if not path.exists():
            # payload disappeared mid-run -> treat as not found, flag recovery
            log.error("missing payload for %s/%s version=%s",
                      bucket, key, meta.version_id)
            raise error(ErrorCode.OBJECT_NOT_FOUND, f"key {key!r} payload unavailable")

        async def _reader():
            async for block in blobstore.iter_file(
                    path, self.settings.stream_chunk_size):
                yield block

        return meta, _reader()

    @staticmethod
    def _meta_or_404(rec: dict, bucket: str, key: str) -> ObjectMeta:
        meta = rec["objects"].get(key)
        if meta is None:
            raise error(ErrorCode.OBJECT_NOT_FOUND,
                        f"key {key!r} not found in bucket {bucket!r}")
        return ObjectMeta.model_validate(meta)

    async def head_object(self, bucket, key, principal) -> ObjectMeta:
        self.layout.validate_key(key)
        rec = self._load_bucket_record(bucket)
        self._check_bucket_action(rec, principal, "s3:GetObject")
        return self._meta_or_404(rec, bucket, key)

    async def delete_object(self, bucket, key, principal) -> bool:
        self.layout.validate_key(key)
        rec = self._load_bucket_record(bucket)
        self._check_bucket_action(rec, principal, "s3:DeleteObject")
        key_lock = await self._key_lock(bucket, key)
        async with key_lock:
            bucket_lock = await self._bucket_lock(bucket)
            async with bucket_lock:
                fresh = self._load_bucket_record(bucket)
                existing = fresh["objects"].pop(key, None)
                if existing is None:
                    return False  # idempotent: nothing to delete
                blob = self.layout.version_blob(
                    bucket, key, existing["version_id"])
                await self._save_bucket_record(fresh)   # invisible from this point
            await async_remove_file(blob)
            obj_dir = self.layout.object_dir(bucket, key)
            if obj_dir.exists() and not any(obj_dir.iterdir()):
                obj_dir.rmdir()
        log.info("delete_object bucket=%s key=%r by=%s",
                 bucket, key, principal.name)
        return True

    # ============ bucket policy ============

    async def put_bucket_policy(self, bucket, principal, policy) -> None:
        lock = await self._bucket_lock(bucket)
        async with lock:
            rec = self._load_bucket_record(bucket)
            # policy administration is owner-only
            if rec["owner"] != principal.name:
                raise error(ErrorCode.ACCESS_DENIED, "only the owner may set policy")
            rec["policy"] = policy.model_dump()
            await self._save_bucket_record(rec)
            log.info("put_bucket_policy bucket=%s statements=%d by=%s",
                     bucket, len(policy.statements), principal.name)

    async def get_bucket_policy(self, bucket, principal) -> BucketPolicy:
        rec = self._load_bucket_record(bucket)
        if rec["owner"] != principal.name:
            raise error(ErrorCode.ACCESS_DENIED, "only the owner may read policy")
        return self._policy_of(rec)

    # ============ multipart upload ============

    MAX_PART_NUMBER = 10_000

    async def initiate_upload(self, bucket, key, principal,
                              content_type=None) -> InitiateUploadResult:
        self.layout.validate_key(key)
        rec = self._load_bucket_record(bucket)  # 404 if bucket missing
        self._check_bucket_action(rec, principal, "s3:PutObject")
        key_lock = await self._key_lock(bucket, key)
        async with key_lock:
            bucket_lock = await self._bucket_lock(bucket)
            async with bucket_lock:
                fresh = self._load_bucket_record(bucket)
                active = fresh["active_uploads"].setdefault(key, {})
                if len(active) >= self.settings.max_uploads_per_object:
                    raise error(
                        ErrorCode.QUOTA_EXCEEDED,
                        f"key {key!r} has {len(active)} active uploads; "
                        f"limit is {self.settings.max_uploads_per_object}",
                    )
                upload_id = uuid.uuid4().hex
                self.layout.upload_parts_dir(bucket, key, upload_id).mkdir(
                    parents=True, exist_ok=True)
                meta = {
                    "upload_id": upload_id,
                    "bucket": bucket,
                    "key": key,
                    "content_type": content_type,
                    "created_at": time.time(),
                    "parts": {},   # part_number -> {size, etag}
                }
                await async_write_json(
                    self.layout.upload_meta_file(bucket, key, upload_id), meta)
                active[upload_id] = {"created_at": meta["created_at"],
                                     "content_type": content_type}
                await self._save_bucket_record(fresh)
        log.info("initiate_upload bucket=%s key=%r upload=%s by=%s",
                 bucket, key, upload_id[:8], principal.name)
        return InitiateUploadResult(bucket=bucket, key=key, upload_id=upload_id)

    async def _load_upload(self, bucket, key, upload_id, write=False):
        """Return (rec, upload_meta) or raise 404. Caller usually holds key lock."""
        rec = self._load_bucket_record(bucket)
        if key not in rec["active_uploads"] or \
                upload_id not in rec["active_uploads"][key]:
            raise error(ErrorCode.UPLOAD_NOT_FOUND,
                        f"upload {upload_id[:8]!r} not found (finished, aborted, "
                        "or never initiated)")
        meta_file = self.layout.upload_meta_file(bucket, key, upload_id)
        if not meta_file.exists():
            # record says active but meta.json is gone -> inconsistent
            raise error(ErrorCode.UPLOAD_NOT_FOUND, "upload metadata missing")
        return rec, read_json(meta_file)

    async def upload_part(self, bucket, key, upload_id, part_number,
                          principal, chunks):
        self.layout.validate_key(key)
        if not (1 <= part_number <= self.MAX_PART_NUMBER):
            raise error(ErrorCode.INVALID_ARGUMENT,
                        f"part_number must be in 1..{self.MAX_PART_NUMBER}")
        rec = self._load_bucket_record(bucket)
        self._check_bucket_action(rec, principal, "s3:PutObject")
        key_lock = await self._key_lock(bucket, key)
        async with key_lock:
            rec, up = await self._load_upload(bucket, key, upload_id)
            parts_dir = self.layout.upload_parts_dir(bucket, key, upload_id)
            tmp, size, digest = await blobstore.stream_sink(
                parts_dir, chunks,
                max_bytes=self.settings.max_object_size,
                chunk_size=self.settings.stream_chunk_size,
            )
            # Idempotent retransmission: same part number + identical etag
            # just replaces the temp file, never double-counted.
            existing = up["parts"].get(str(part_number))
            if existing is not None and existing["etag"] != digest:
                await async_remove_file(tmp)
                raise error(
                    ErrorCode.CONFLICT,
                    f"part {part_number} already uploaded with a different "
                    "etag; abort or retransmit identical bytes",
                )
            import os
            final = self.layout.part_file(bucket, key, upload_id, part_number)
            await asyncio.to_thread(os.replace, tmp, final)
            up["parts"][str(part_number)] = {"size": size, "etag": digest}
            await async_write_json(
                self.layout.upload_meta_file(bucket, key, upload_id), up)
        log.info("upload_part bucket=%s key=%r upload=%s part=%d size=%d etag=%s",
                 bucket, key, upload_id[:8], part_number, size, digest[:8])
        return part_number, digest

    async def list_parts(self, bucket, key, upload_id, principal):
        self.layout.validate_key(key)
        rec = self._load_bucket_record(bucket)
        self._check_bucket_action(rec, principal, "s3:ListBucket")
        key_lock = await self._key_lock(bucket, key)
        async with key_lock:
            _, up = await self._load_upload(bucket, key, upload_id)
        from app.models import PartInfo
        parts = [PartInfo(part_number=int(n), **info)
                 for n, info in sorted(up["parts"].items(), key=lambda x: int(x[0]))]
        return ListPartsResult(bucket=bucket, key=key,
                               upload_id=upload_id, parts=parts)

    async def abort_upload(self, bucket, key, upload_id, principal) -> None:
        self.layout.validate_key(key)
        rec = self._load_bucket_record(bucket)
        self._check_bucket_action(rec, principal, "s3:PutObject")
        key_lock = await self._key_lock(bucket, key)
        async with key_lock:
            rec = self._load_bucket_record(bucket)
            if key not in rec["active_uploads"] or \
                    upload_id not in rec["active_uploads"].get(key, {}):
                # idempotent abort (also covers post-restart cleanup)
                if not self.layout.upload_dir(bucket, key, upload_id).exists():
                    raise error(ErrorCode.UPLOAD_NOT_FOUND,
                                f"upload {upload_id[:8]!r} not found")
                await async_remove_dir(self.layout.upload_dir(bucket, key, upload_id))
                return
            await self._finalize_abort(bucket, key, upload_id, rec)
        log.info("abort_upload bucket=%s key=%r upload=%s by=%s",
                 bucket, key, upload_id[:8], principal.name)

    async def _finalize_abort(self, bucket, key, upload_id, rec) -> None:
        rec["active_uploads"].get(key, {}).pop(upload_id, None)
        if not rec["active_uploads"].get(key):
            rec["active_uploads"].pop(key, None)
        await self._save_bucket_record(rec)
        await async_remove_dir(self.layout.upload_dir(bucket, key, upload_id))

    async def complete_upload(self, bucket, key, upload_id, principal,
                              parts=None) -> CompleteUploadResult:
        """Merge validated parts in number order; exactly-once publication.

        ``parts`` is the client's ordered manifest
        [{"part_number": n, "etag": e}, ...]; when omitted, all stored parts
        in ascending number order are used. Parts may have *arrived* in any
        order, but assembly is always strictly ascending.
        """
        self.layout.validate_key(key)
        rec = self._load_bucket_record(bucket)
        self._check_bucket_action(rec, principal, "s3:PutObject")
        key_lock = await self._key_lock(bucket, key)
        async with key_lock:
            # idempotent re-complete after a client timeout
            rec = self._load_bucket_record(bucket)
            marker = rec["completed_uploads"].get(upload_id)
            if marker is not None:
                existing = rec["objects"].get(key)
                if existing is not None and \
                        existing["version_id"] == marker["version_id"]:
                    return CompleteUploadResult(
                        bucket=bucket, key=key, etag=existing["etag"],
                        version_id=existing["version_id"],
                        size=existing["size"])
                # marker without object (shouldn't happen): fall through/rebuild

            rec, up = await self._load_upload(bucket, key, upload_id)
            stored = up["parts"]

            if parts is None:
                ordered_nums = sorted(stored, key=int)
                manifest = [(int(n), stored[n]["etag"]) for n in ordered_nums]
            else:
                manifest = []
                prev = 0
                for p in parts:
                    n = int(p["part_number"])
                    if n <= prev:
                        raise error(ErrorCode.INVALID_PART_ORDER,
                                    "parts must be listed in strictly ascending "
                                    "part_number order")
                    prev = n
                    info = stored.get(str(n))
                    if info is None:
                        raise error(ErrorCode.INVALID_PART,
                                    f"part {n} was never uploaded")
                    if p.get("etag") and p["etag"] != info["etag"]:
                        raise error(ErrorCode.INVALID_PART,
                                                    f"etag mismatch for part {n}")
                    manifest.append((n, info["etag"]))

            if not manifest:
                raise error(ErrorCode.INVALID_PART,
                            "cannot complete upload with zero parts")
            # size bounds: per-part minimum applies to all but the last
            min_part = self.settings.min_part_size
            total = 0
            for index, (n, _etag) in enumerate(manifest):
                info = stored[str(n)]
                total += info["size"]
                if index < len(manifest) - 1 and info["size"] < min_part:
                    raise error(
                        ErrorCode.ENTITY_TOO_LARGE,
                        f"part {n} is {info['size']} bytes; all but the last "
                        f"part must be >= {min_part} bytes "
                        "(configure OSG_MIN_PART_SIZE)",
                    )
            if total > self.settings.max_object_size:
                raise error(ErrorCode.ENTITY_TOO_LARGE,
                            f"assembled object exceeds "
                            f"{self.settings.max_object_size} bytes")

            await self._enter_write_gate(bucket)
            assembled = None
            try:
                obj_dir = self.layout.object_dir(bucket, key)
                obj_dir.mkdir(parents=True, exist_ok=True)
                part_paths = [self.layout.part_file(bucket, key, upload_id, n)
                              for n, _etag in manifest]
                # deterministic merge-temp name (one upload is key-lock serialized)
                merge_tmp = obj_dir / ".merge.incoming.part"
                size, etag = await blobstore.async_concat_parts(
                    part_paths, merge_tmp, self.settings.stream_chunk_size)
                assembled = merge_tmp

                # publish under bucket lock, with quota recheck on fresh record
                try:
                    meta = await self._publish_version(
                        bucket, key, rec, assembled, size, etag,
                        content_type=up.get("content_type"))
                except BaseException:
                    await async_remove_file(assembled)
                    raise
                assembled = None  # consumed

                # mark completion + remove upload state atomically-ish:
                bucket_lock = await self._bucket_lock(bucket)
                async with bucket_lock:
                    fresh = self._load_bucket_record(bucket)
                    fresh["active_uploads"].get(key, {}).pop(upload_id, None)
                    if not fresh["active_uploads"].get(key):
                        fresh["active_uploads"].pop(key, None)
                    fresh["completed_uploads"][upload_id] = {
                        "version_id": meta.version_id, "completed_at": time.time()}
                    fresh["completed_uploads"] = dict(
                        list(fresh["completed_uploads"].items())[-1000:])
                    await self._save_bucket_record(fresh)
            finally:
                if assembled is not None:
                    await async_remove_file(assembled)
                await self._leave_write_gate(bucket)
            await async_remove_dir(self.layout.upload_dir(bucket, key, upload_id))
        log.info("complete_upload bucket=%s key=%r upload=%s parts=%d size=%d etag=%s",
                 bucket, key, upload_id[:8], len(manifest), size, etag[:8])
        return CompleteUploadResult(bucket=bucket, key=key, etag=etag,
                                    version_id=meta.version_id, size=size)
