"""Recovery after crash/restart reconciles metadata and payloads."""
from __future__ import annotations

import json
import logging
from pathlib import Path

import pytest

from app.auth import Principal
from app.config import load_settings
from app.errors import ErrorCode, GatewayError
from app.metastore import atomic_write_json
from app.store import GatewayStore

log = logging.getLogger("test.recovery")
ALICE = Principal("alice")


async def _chunks(parts):
    for p in parts:
        yield p


async def _read(store, bucket, key):
    _, stream = await store.get_object(bucket, key, ALICE)
    return b"".join([c async for c in stream])


async def test_restart_aborts_dangling_uploads(store: GatewayStore,
                                               tmp_path: Path):
    await store.create_bucket("rb", ALICE)
    up = await store.initiate_upload("rb", "open", ALICE)
    await store.upload_part("rb", "open", up.upload_id, 1,
                            ALICE, _chunks([b"AAAA"]))
    await store.put_object("rb", "done", ALICE, _chunks([b"payload"]))
    data_dir = store.layout.root

    # restart: a brand new process/store over the same directory
    store2 = GatewayStore(load_settings(str(data_dir)))
    report = await store2.start()
    log.info("input: restart with 1 open upload -> report=%s", report)
    assert report["aborted_uploads"] >= 1
    assert not any(store2.layout.uploads_dir.rglob("*.part"))
    with pytest.raises(GatewayError) as ei:
        await store2.list_parts("rb", "open", up.upload_id, ALICE)
    assert ei.value.code is ErrorCode.UPLOAD_NOT_FOUND
    assert await _read(store2, "rb", "done") == b"payload"


async def test_stray_temp_and_incoming_files_removed(store: GatewayStore):
    await store.create_bucket("rb2", ALICE)
    await store.put_object("rb2", "k", ALICE, _chunks([b"v"]))
    root = store.layout.root
    (root / "objects" / "rb2" / "k" / ".abc.part.tmp").write_bytes(b"x")
    (root / "uploads").mkdir(exist_ok=True)
    (root / "uploads" / ".incoming-9.part").write_bytes(b"y")
    (root / "buckets" / ".rb2.json.123.tmp").write_bytes(b"z")
    store2 = GatewayStore(load_settings(str(root)))
    report = await store2.start()
    log.info("decision: removed %d stray temp files on startup",
             report["stray_tmp"])
    assert report["stray_tmp"] == 3
    assert not list(root.rglob(".*.tmp"))
    assert not list(root.rglob(".incoming-*"))


async def test_metadata_without_payload_is_dropped(store: GatewayStore):
    await store.create_bucket("rb3", ALICE)
    await store.put_object("rb3", "real", ALICE, _chunks([b"ok"]))
    # corrupt the record: inject a key whose blob does not exist
    rec_path = store.layout.bucket_file("rb3")
    rec = json.loads(rec_path.read_text())
    rec["objects"]["ghost"] = {
        "bucket": "rb3", "key": "ghost", "size": 5,
        "etag": "f" * 64, "version_id": "deadbeef",
        "created_at": 0.0, "updated_at": 0.0, "content_type": None}
    atomic_write_json(rec_path, rec)

    store2 = GatewayStore(load_settings(str(store.layout.root)))
    report = await store2.start()
    log.info("decision: %d metadata-without-payload entries dropped",
             report["orphan_meta"])
    assert report["orphan_meta"] == 1
    listed = await store2.list_objects("rb3", ALICE)
    assert [o.key for o in listed.objects] == ["real"]
    assert await _read(store2, "rb3", "real") == b"ok"


async def test_orphan_blob_is_reaped(store: GatewayStore):
    await store.create_bucket("rb4", ALICE)
    obj_dir = store.layout.object_dir("rb4", "unreferenced")
    obj_dir.mkdir(parents=True)
    (obj_dir / "orphan.part").write_bytes(b"data")
    store2 = GatewayStore(load_settings(str(store.layout.root)))
    report = await store2.start()
    log.info("decision: %d orphan blobs reaped", report["orphan_blobs"])
    assert report["orphan_blobs"] == 1
    assert not obj_dir.exists()
