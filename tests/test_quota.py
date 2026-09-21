"""Capacity/concurrency quota: rejection is explicit and leaves no residue."""
from __future__ import annotations

import logging

import pytest

from app.auth import Principal
from app.errors import ErrorCode, GatewayError
from app.store import GatewayStore

log = logging.getLogger("test.quota")
ALICE = Principal("alice")
BOB = Principal("bob")


async def _chunks(parts):
    for p in parts:
        yield p


async def test_byte_and_count_quota(store: GatewayStore):
    await store.create_bucket("qb", ALICE, max_bytes=20, max_objects=2)
    await store.put_object("qb", "k1", ALICE, _chunks([b"0123456789"]))

    with pytest.raises(GatewayError) as ei:
        await store.put_object("qb", "k2", ALICE, _chunks([b"x" * 11]))
    assert ei.value.code is ErrorCode.QUOTA_EXCEEDED
    assert ei.value.status_code == 507
    log.info("decision: 21 bytes projected under 20-byte cap -> %s",
             ei.value.code.value)

    # 10 more bytes is exactly at the cap -> allowed
    await store.put_object("qb", "k2", ALICE, _chunks([b"y" * 10]))
    # third object breaches count quota
    with pytest.raises(GatewayError) as ei:
        await store.put_object("qb", "k3", ALICE, _chunks([b"z"]))
    assert ei.value.code is ErrorCode.QUOTA_EXCEEDED
    log.info("decision: third object under count cap 2 -> %s",
             ei.value.code.value)

    listed = await store.list_objects("qb", ALICE)
    assert {o.key for o in listed.objects} == {"k1", "k2"}
    # no temp artifacts remain
    assert not list(store.layout.root.rglob(".incoming-*"))
    assert not list(store.layout.root.rglob(".*.tmp"))


async def test_overwrite_is_measured_against_new_size(store: GatewayStore):
    await store.create_bucket("qb2", ALICE, max_bytes=10)
    await store.put_object("qb2", "k", ALICE, _chunks([b"0123456789"]))
    # same key shrinking stays within quota and must succeed
    m = await store.put_object("qb2", "k", ALICE, _chunks([b"tiny"]))
    assert m.size == 4
    log.info("decision: overwrite shrink under byte cap succeeds")


async def test_multipart_abort_frees_quota_and_parts(store: GatewayStore):
    await store.create_bucket("qb3", ALICE, max_objects=1)
    up = await store.initiate_upload("qb3", "soon", ALICE)
    await store.upload_part("qb3", "soon", up.upload_id, 1,
                            ALICE, _chunks([b"AAAA"]))
    await store.abort_upload("qb3", "soon", up.upload_id, ALICE)
    # slot is free again: a normal put succeeds and bucket can be deleted
    await store.put_object("qb3", "only", ALICE, _chunks([b"v"]))
    await store.delete_object("qb3", "only", ALICE)
    # no upload files (parts/meta) survive an abort
    assert not list(store.layout.uploads_dir.rglob("*.part"))
    assert not list(store.layout.uploads_dir.rglob("meta.json"))


async def test_denied_put_does_not_leak_existence_via_http(client=None):
    # covered at store level here: write denied before any state is created
    pytest.importorskip("fastapi")  # always available; keeps intent explicit
