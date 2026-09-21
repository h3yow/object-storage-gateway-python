"""Bucket/object lifecycle success and failure paths."""
from __future__ import annotations

import logging

import pytest

from app.auth import Principal
from app.errors import ErrorCode, GatewayError
from app.store import GatewayStore

log = logging.getLogger("test.lifecycle")
ALICE = Principal("alice")


async def _chunks(parts):
    for p in parts:
        yield p


async def _read(get_res):
    meta, stream = get_res
    body = b"".join([c async for c in stream])
    return meta, body


async def test_bucket_create_conflict_and_missing(store: GatewayStore):
    await store.create_bucket("bkt-1", ALICE)
    log.info("input: create bkt-1 twice -> expect %s", ErrorCode.BUCKET_EXISTS.value)
    with pytest.raises(GatewayError) as ei:
        await store.create_bucket("bkt-1", ALICE)
    assert ei.value.code is ErrorCode.BUCKET_EXISTS
    assert ei.value.status_code == 409

    log.info("input: delete missing bucket -> expect %s",
             ErrorCode.BUCKET_NOT_FOUND.value)
    with pytest.raises(GatewayError) as ei:
        await store.delete_bucket("nope", ALICE)
    assert ei.value.code is ErrorCode.BUCKET_NOT_FOUND
    assert ei.value.status_code == 404


async def test_invalid_bucket_and_key_names(store: GatewayStore):
    with pytest.raises(GatewayError) as ei:
        await store.create_bucket("UPPER", ALICE)
    assert ei.value.code is ErrorCode.INVALID_ARGUMENT
    with pytest.raises(GatewayError) as ei:
        await store.put_object("bkt-1", "", ALICE, _chunks([b"x"]))
    assert ei.value.code is ErrorCode.INVALID_ARGUMENT


async def test_object_overwrite_get_delete(store: GatewayStore):
    await store.create_bucket("bkt-2", ALICE)
    m1 = await store.put_object("bkt-2", "k", ALICE, _chunks([b"v1"]))
    log.info("input: put k v1 -> etag=%s version=%s",
             m1.etag[:8], m1.version_id[:8])

    # object missing -> 404, distinct from permission failure
    with pytest.raises(GatewayError) as ei:
        await store.get_object("bkt-2", "missing", ALICE)
    assert ei.value.code is ErrorCode.OBJECT_NOT_FOUND

    m2 = await store.put_object("bkt-2", "k", ALICE, _chunks([b"v2-longer"]))
    assert m2.version_id != m1.version_id, "overwrite creates new version"
    meta, body = await _read(await store.get_object("bkt-2", "k", ALICE))
    log.info("decision: current version=%s -> body=%r", meta.version_id[:8], body)
    assert body == b"v2-longer"
    assert meta.size == len(body)

    # delete reports true once, then idempotent false
    assert await store.delete_object("bkt-2", "k", ALICE) is True
    assert await store.delete_object("bkt-2", "k", ALICE) is False
    with pytest.raises(GatewayError) as ei:
        await store.get_object("bkt-2", "k", ALICE)
    assert ei.value.code is ErrorCode.OBJECT_NOT_FOUND
    # empty bucket deletable after objects removed
    await store.delete_bucket("bkt-2", ALICE)


async def test_bucket_not_empty_blocks_delete(store: GatewayStore):
    await store.create_bucket("bkt-3", ALICE)
    await store.put_object("bkt-3", "k", ALICE, _chunks([b"v"]))
    log.info("input: delete bucket with 1 object -> expect %s",
             ErrorCode.BUCKET_NOT_EMPTY.value)
    with pytest.raises(GatewayError) as ei:
        await store.delete_bucket("bkt-3", ALICE)
    assert ei.value.code is ErrorCode.BUCKET_NOT_EMPTY
    assert ei.value.status_code == 409


async def test_listing_prefix_and_pagination(store: GatewayStore):
    await store.create_bucket("bkt-4", ALICE)
    for i in range(5):
        await store.put_object("bkt-4", f"logs/2026/{i}.log",
                               ALICE, _chunks([str(i).encode()]))
    await store.put_object("bkt-4", "readme", ALICE, _chunks([b"r"]))

    page1 = await store.list_objects("bkt-4", ALICE, prefix="", max_keys=3)
    log.info("decision: page1 keys=%s next=%s truncated=%s",
             [o.key for o in page1.objects], page1.next_marker, page1.truncated)
    assert page1.truncated is True
    page2 = await store.list_objects(
        "bkt-4", ALICE, prefix="", marker=page1.next_marker, max_keys=3)
    keys = [o.key for o in page1.objects] + [o.key for o in page2.objects]
    assert keys == sorted(keys) and len(keys) == 6

    pre = await store.list_objects("bkt-4", ALICE, prefix="logs/", max_keys=10)
    assert {o.key for o in pre.objects} == {f"logs/2026/{i}.log" for i in range(5)}

    delim = await store.list_objects(
        "bkt-4", ALICE, prefix="", delimiter="/")
    assert "logs/" in delim.common_prefixes
    assert [o.key for o in delim.objects] == ["readme"]
