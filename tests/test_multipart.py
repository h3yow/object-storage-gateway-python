"""Multipart: out-of-order parts, retransmission, abort, merge, idempotency."""
from __future__ import annotations

import logging

import pytest

from app.auth import Principal
from app.errors import ErrorCode, GatewayError
from app.store import GatewayStore

log = logging.getLogger("test.multipart")
ALICE = Principal("alice")


async def _chunks(parts):
    for p in parts:
        yield p


async def _read(store, bucket, key):
    meta, stream = await store.get_object(bucket, key, ALICE)
    return meta, b"".join([c async for c in stream])


async def test_parts_out_of_order_then_ordered_merge(store: GatewayStore):
    await store.create_bucket("mp", ALICE)
    up = await store.initiate_upload("mp", "big", ALICE)
    uid = up.upload_id
    log.info("input: initiate upload=%s; parts arrive 3,1,2 (out of order)",
             uid[:8])

    _, e3 = await store.upload_part("mp", "big", uid, 3, ALICE, _chunks([b"CCCC"]))
    _, e1 = await store.upload_part("mp", "big", uid, 1, ALICE, _chunks([b"AAAA"]))
    _, e2 = await store.upload_part("mp", "big", uid, 2, ALICE, _chunks([b"BBBB"]))

    parts = await store.list_parts("mp", "big", uid, ALICE)
    # listing is always by part number even though arrival was unordered
    assert [p.part_number for p in parts.parts] == [1, 2, 3]
    log.info("decision: stored part order=%s (sorted regardless of arrival)",
             [p.part_number for p in parts.parts])

    # unordered manifest rejected
    with pytest.raises(GatewayError) as ei:
        await store.complete_upload(
            "mp", "big", uid, ALICE,
            [{"part_number": 2, "etag": e2}, {"part_number": 1, "etag": e1}])
    assert ei.value.code is ErrorCode.INVALID_PART_ORDER

    # manifest referencing an etag that differs is rejected
    with pytest.raises(GatewayError) as ei:
        await store.complete_upload(
            "mp", "big", uid, ALICE,
            [{"part_number": 1, "etag": "deadbeef"},
             {"part_number": 2, "etag": e2}, {"part_number": 3, "etag": e3}])
    assert ei.value.code is ErrorCode.INVALID_PART

    res = await store.complete_upload(
        "mp", "big", uid, ALICE,
        [{"part_number": 1, "etag": e1}, {"part_number": 2, "etag": e2},
         {"part_number": 3, "etag": e3}])
    _, body = await _read(store, "mp", "big")
    log.info("decision: merged size=%d etag=%s body=%r",
             res.size, res.etag[:8], body)
    assert body == b"AAAABBBBCCCC"


async def test_part_retransmission_is_idempotent(store: GatewayStore):
    await store.create_bucket("mp", ALICE)
    uid = (await store.initiate_upload("mp", "k", ALICE)).upload_id
    _, etag1 = await store.upload_part("mp", "k", uid, 1, ALICE, _chunks([b"AAAA"]))
    _, etag1b = await store.upload_part("mp", "k", uid, 1, ALICE, _chunks([b"AAAA"]))
    log.info("input: retransmit part1 same bytes -> etag1=%s etag1b=%s",
             etag1[:8], etag1b[:8])
    assert etag1 == etag1b
    parts = await store.list_parts("mp", "k", uid, ALICE)
    assert len(parts.parts) == 1, "duplicate submission must not double count"
    assert parts.parts[0].size == 4

    # different bytes on the same part number -> explicit conflict
    log.info("input: retransmit part1 different bytes -> expect %s",
             ErrorCode.CONFLICT.value)
    with pytest.raises(GatewayError) as ei:
        await store.upload_part("mp", "k", uid, 1, ALICE, _chunks([b"OTHER"]))
    assert ei.value.code is ErrorCode.CONFLICT
    parts = await store.list_parts("mp", "k", uid, ALICE)
    assert parts.parts[0].size == 4, "conflicting part must not replace stored one"


async def test_unfinished_upload_is_invisible(store: GatewayStore):
    await store.create_bucket("mp", ALICE)
    uid = (await store.initiate_upload("mp", "ghost", ALICE)).upload_id
    await store.upload_part("mp", "ghost", uid, 1, ALICE, _chunks([b"AAAA"]))
    with pytest.raises(GatewayError) as ei:
        await store.head_object("mp", "ghost", ALICE)
    assert ei.value.code is ErrorCode.OBJECT_NOT_FOUND
    listed = await store.list_objects("mp", ALICE)
    assert listed.objects == []


async def test_complete_is_idempotent_after_timeout(store: GatewayStore):
    await store.create_bucket("mp", ALICE)
    uid = (await store.initiate_upload("mp", "k", ALICE)).upload_id
    _, e1 = await store.upload_part("mp", "k", uid, 1, ALICE, _chunks([b"AAAA"]))
    r1 = await store.complete_upload(
        "mp", "k", uid, ALICE, [{"part_number": 1, "etag": e1}])
    # client retries after a timeout — must not amplify side effects
    r2 = await store.complete_upload(
        "mp", "k", uid, ALICE, [{"part_number": 1, "etag": e1}])
    log.info("decision: repeated complete -> same version %s == %s",
             r1.version_id[:8], r2.version_id[:8])
    assert r1.version_id == r2.version_id
    listed = await store.list_objects("mp", ALICE)
    assert len(listed.objects) == 1


async def test_abort_removes_everything_and_is_safe_to_repeat(
        store: GatewayStore):
    await store.create_bucket("mp", ALICE)
    uid = (await store.initiate_upload("mp", "k", ALICE)).upload_id
    await store.upload_part("mp", "k", uid, 1, ALICE, _chunks([b"AAAA"]))
    await store.abort_upload("mp", "k", uid, ALICE)
    parts_root = store.layout.upload_parts_dir("mp", "k", uid)
    assert not parts_root.exists()
    log.info("decision: abort removed upload dir; bucket can be deleted")
    # repeat abort -> distinguishable 404, no crash, no residue
    with pytest.raises(GatewayError) as ei:
        await store.abort_upload("mp", "k", uid, ALICE)
    assert ei.value.code is ErrorCode.UPLOAD_NOT_FOUND
    await store.delete_bucket("mp", ALICE)


async def test_complete_requires_existing_and_nonempty_parts(
        store: GatewayStore):
    await store.create_bucket("mp", ALICE)
    uid = (await store.initiate_upload("mp", "k", ALICE)).upload_id
    with pytest.raises(GatewayError) as ei:
        await store.complete_upload("mp", "k", uid, ALICE, [])
    assert ei.value.code is ErrorCode.INVALID_PART
    with pytest.raises(GatewayError) as ei:
        await store.complete_upload(
            "mp", "k", uid, ALICE, [{"part_number": 9, "etag": "x"}])
    assert ei.value.code is ErrorCode.INVALID_PART
    await store.abort_upload("mp", "k", uid, ALICE)
