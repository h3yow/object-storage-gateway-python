"""Per-action authorization; denied callers cannot probe resource existence."""
from __future__ import annotations

import logging

import pytest

from app.auth import Principal
from app.errors import ErrorCode, GatewayError
from app.models import BucketPolicy, PolicyStatement
from app.store import GatewayStore

log = logging.getLogger("test.authz")
ALICE = Principal("alice")
BOB = Principal("bob")
CAROL = Principal("carol")


async def _chunks(parts):
    for p in parts:
        yield p


async def test_actions_are_checked_separately(store: GatewayStore):
    await store.create_bucket("sec", ALICE)
    # Bob may read but nothing else; Carol may list only
    policy = BucketPolicy(statements=[
        PolicyStatement(effect="allow", principal="bob",
                        actions=["s3:GetObject"]),
        PolicyStatement(effect="allow", principal="carol",
                        actions=["s3:ListBucket"]),
    ])
    await store.put_bucket_policy("sec", ALICE, policy)
    await store.put_object("sec", "k", ALICE, _chunks([b"v"]))

    _, stream = await store.get_object("sec", "k", BOB)
    assert b"".join([c async for c in stream]) == b"v"
    log.info("decision: bob GetObject allowed by policy")

    for action_name, coro in [
        ("PutObject", store.put_object("sec", "k2", BOB, _chunks([b"x"]))),
        ("DeleteObject", store.delete_object("sec", "k", BOB)),
        ("ListBucket", store.list_objects("sec", BOB)),
    ]:
        with pytest.raises(GatewayError) as ei:
            await coro
        assert ei.value.code is ErrorCode.ACCESS_DENIED
        assert ei.value.status_code == 403
        log.info("decision: bob %s denied (403)", action_name)

    # carol: list allowed, get denied
    res = await store.list_objects("sec", CAROL)
    assert [o.key for o in res.objects] == ["k"]
    with pytest.raises(GatewayError) as ei:
        await store.get_object("sec", "k", CAROL)
    assert ei.value.code is ErrorCode.ACCESS_DENIED


async def test_deny_overrides_allow_and_owner_wins(store: GatewayStore):
    await store.create_bucket("sec2", ALICE)
    policy = BucketPolicy(statements=[
        PolicyStatement(effect="allow", principal="bob", actions=["*"]),
        PolicyStatement(effect="deny", principal="bob",
                        actions=["s3:DeleteObject"]),
    ])
    await store.put_bucket_policy("sec2", ALICE, policy)
    await store.put_object("sec2", "k", ALICE, _chunks([b"v"]))
    # wildcard allow grants get ...
    _, stream = await store.get_object("sec2", "k", BOB)
    assert b"".join([c async for c in stream]) == b"v"
    # ... but explicit deny wins for delete
    with pytest.raises(GatewayError) as ei:
        await store.delete_object("sec2", "k", BOB)
    assert ei.value.code is ErrorCode.ACCESS_DENIED
    log.info("decision: explicit deny beats wildcard allow")
    # owner remains all-powerful despite deny statement
    assert await store.delete_object("sec2", "k", ALICE) is True


async def test_denied_caller_cannot_distinguish_missing_key(
        store: GatewayStore):
    await store.create_bucket("sec3", ALICE)
    await store.put_object("sec3", "present", ALICE, _chunks([b"v"]))
    # Bob has no statements at all: both keys must return identical 403
    for key in ("present", "absent-key"):
        with pytest.raises(GatewayError) as ei:
            await store.get_object("sec3", key, BOB)
        assert ei.value.code is ErrorCode.ACCESS_DENIED
    log.info("decision: denied GET for present and missing keys both -> "
             "AccessDenied (existence not leaked)")
    # and the authorized owner still gets a distinguishable 404
    with pytest.raises(GatewayError) as ei:
        await store.get_object("sec3", "absent-key", ALICE)
    assert ei.value.code is ErrorCode.OBJECT_NOT_FOUND


async def test_policy_admin_is_owner_only(store: GatewayStore):
    await store.create_bucket("sec4", ALICE)
    with pytest.raises(GatewayError) as ei:
        await store.put_bucket_policy(
            "sec4", BOB, BucketPolicy(statements=[]))
    assert ei.value.code is ErrorCode.ACCESS_DENIED
    with pytest.raises(GatewayError) as ei:
        await store.get_bucket_policy("sec4", BOB)
    assert ei.value.code is ErrorCode.ACCESS_DENIED


async def test_anonymous_request_rejected_at_http_layer():
    # unit-level: header parsing produces a stable 401, separate from 403
    from app.auth import principal_from_headers
    with pytest.raises(GatewayError) as ei:
        principal_from_headers({})
    assert ei.value.code is ErrorCode.UNAUTHORIZED
    assert ei.value.status_code == 401
