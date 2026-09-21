"""Concurrent writes are serialized; conditional writes reject stale updates."""
from __future__ import annotations

import asyncio
import logging

import pytest

from app.auth import Principal
from app.errors import ErrorCode, GatewayError
from app.store import GatewayStore

log = logging.getLogger("test.concurrency")
ALICE = Principal("alice")


async def _chunks(parts):
    for p in parts:
        yield p


async def _read(store, bucket, key):
    _, stream = await store.get_object(bucket, key, ALICE)
    return b"".join([c async for c in stream])


async def test_unconditional_concurrent_puts_never_lose_or_tear(
        store: GatewayStore):
    """N writers overwrite one key: final state must be one whole version."""
    await store.create_bucket("cb", ALICE)
    n = 20

    async def writer(i: int):
        payload = bytes([65 + (i % 26)]) * (1000 + i)
        await store.put_object("cb", "hot", ALICE, _chunks([payload]))

    await asyncio.gather(*(writer(i) for i in range(n)))
    body = await _read(store, "cb", "hot")
    log.info("decision: %d concurrent writes; final size=%d uniform=%s",
             n, len(body), len(set(body)) == 1)
    assert len(set(body)) == 1, "torn/interleaved bytes observed"
    assert len(body) >= 1000
    assert len(body) <= 1000 + n - 1
    # only current blob remains (old versions retired, no orphans)
    key_dir = store.layout.object_dir("cb", "hot")
    assert len(list(key_dir.glob("*.part"))) == 1


async def test_conditional_cas_serializes_writers(store: GatewayStore):
    """CAS pattern: racing writers serialize; stale etags are rejected."""
    await store.create_bucket("cb", ALICE)
    await store.put_object("cb", "counter", ALICE, _chunks([b"0"]))

    # Deterministic collision round: everyone reads the same etag, then all
    # eight commits are issued concurrently -> exactly one may win.
    heads = await asyncio.gather(*[
        store.head_object("cb", "counter", ALICE) for _ in range(8)])
    shared_etag = heads[0].etag
    assert all(h.etag == shared_etag for h in heads)

    async def first_round_commit(i: int):
        try:
            await store.put_object("cb", "counter", ALICE,
                                   _chunks([b"1"]), if_match=shared_etag)
            return "committed"
        except GatewayError as exc:
            assert exc.code is ErrorCode.PRECONDITION_FAILED
            log.info("writer %d: stale etag %s rejected with %s",
                     i, shared_etag[:8], exc.code.value)
            return "rejected"

    outcomes = await asyncio.gather(*(first_round_commit(i) for i in range(8)))
    assert outcomes.count("committed") == 1
    assert outcomes.count("rejected") == 7
    assert await _read(store, "cb", "counter") == b"1"

    # Remaining writers follow the read-modify-write retry loop to finish.
    results = {"committed": 0, "rejected": 0}

    async def cas_writer(i: int):
        for _attempt in range(50):
            head = await store.head_object("cb", "counter", ALICE)
            try:
                next_v = str(int(await _read(store, "cb", "counter")) + 1).encode()
                await store.put_object(
                    "cb", "counter", ALICE, _chunks([next_v]),
                    if_match=head.etag)
                results["committed"] += 1
                return
            except GatewayError as exc:
                if exc.code is not ErrorCode.PRECONDITION_FAILED:
                    raise
                results["rejected"] += 1
        raise AssertionError(f"writer {i} never committed")

    await asyncio.gather(*(cas_writer(i) for i in range(7)))
    final = int(await _read(store, "cb", "counter"))
    log.info("decision: first round 1 commit / 7 reject; retry phase "
             "committed=%d rejected=%d final counter=%d",
             results["committed"], results["rejected"], final)
    assert final == 8
    assert results["committed"] == 7


async def test_if_none_match_star(store: GatewayStore):
    await store.create_bucket("cb", ALICE)
    await store.put_object("cb", "once", ALICE, _chunks([b"a"]))
    log.info("input: If-None-Match:* on existing key -> expect %s",
             ErrorCode.PRECONDITION_FAILED.value)
    with pytest.raises(GatewayError) as ei:
        await store.put_object("cb", "once", ALICE, _chunks([b"b"]),
                               if_none_match="*")
    assert ei.value.code is ErrorCode.PRECONDITION_FAILED
    assert ei.value.status_code == 412
    # original value preserved (no half update)
    assert await _read(store, "cb", "once") == b"a"


async def test_concurrent_writes_never_read_mid_state(store: GatewayStore):
    """Readers always see a full version while writers overwrite."""
    await store.create_bucket("cb", ALICE)
    await store.put_object("cb", "spin", ALICE, _chunks([b"0" * 5000]))
    stop = asyncio.Event()

    async def writer():
        i = 0
        while not stop.is_set():
            payload = bytes([48 + (i % 10)]) * 5000
            await store.put_object("cb", "spin", ALICE, _chunks([payload]))
            i += 1

    async def reader():
        for _ in range(200):
            await asyncio.sleep(0)
            body = await _read(store, "cb", "spin")
            assert len(body) == 5000
            assert len(set(body)) == 1, f"mid-state read: {set(body)!r}"

    writer_task = asyncio.create_task(writer())
    try:
        await reader()
    finally:
        stop.set()
        await writer_task
    log.info("decision: 200 concurrent reads all observed whole versions")


async def test_inflight_quota_rejects_and_leaves_nothing(
        store: GatewayStore, monkeypatch):
    # small gate for a dedicated fresh store via settings override
    await store.create_bucket("qb", ALICE)
    entered = asyncio.Event()
    release = asyncio.Event()
    real_publish = store._publish_version
    calls = {"entered": 0}

    async def slow_publish(bucket, key, rec, tmp, size, etag, content_type=None):
        calls["entered"] += 1
        entered.set()
        await release.wait()
        return await real_publish(bucket, key, rec, tmp, size, etag,
                                  content_type=content_type)

    store._publish_version = slow_publish
    t1 = asyncio.create_task(
        store.put_object("qb", "k1", ALICE, _chunks([b"a" * 10])))
    await entered.wait()
    # gate capacity is 4 (fixture setting)
    others = [asyncio.create_task(store.put_object(
        "qb", f"k{i}", ALICE, _chunks([b"b"]))) for i in range(2, 5)]
    await asyncio.sleep(0.05)
    extra = asyncio.create_task(
        store.put_object("qb", "kX", ALICE, _chunks([b"x"])))
    await asyncio.sleep(0.05)
    assert extra.done()
    with pytest.raises(GatewayError) as ei:
        extra.result()
    log.info("decision: 5th concurrent write rejected -> %s",
             ei.value.code.value)
    assert ei.value.code is ErrorCode.QUOTA_EXCEEDED
    release.set()
    await t1
    await asyncio.gather(*others)
    # rejected request left neither object nor temp file
    listing = await store.list_objects("qb", ALICE)
    assert "kX" not in {o.key for o in listing.objects}
    leftovers = list(store.layout.bucket_object_dir("qb").rglob(".incoming-*"))
    assert leftovers == []
