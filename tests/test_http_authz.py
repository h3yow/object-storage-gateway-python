"""HTTP-layer authz: existence leak prevention and per-action codes."""
from __future__ import annotations

import logging

import pytest
from fastapi.testclient import TestClient

from app.api import create_app
from app.config import load_settings
from app.models import BucketPolicy, PolicyStatement

log = logging.getLogger("test.http_authz")


@pytest.fixture
def client(tmp_path):
    settings = load_settings(str(tmp_path / "data"))
    with TestClient(create_app(settings)) as c:
        yield c


def test_denied_get_identical_for_present_and_missing(client: TestClient):
    alice = {"X-OSG-Principal": "alice"}
    bob = {"X-OSG-Principal": "bob"}
    client.put("/v1/buckets/sec", headers=alice)
    client.put("/v1/buckets/sec/objects/present", content=b"v", headers=alice)
    # bob has no policy grants at all
    r1 = client.get("/v1/buckets/sec/objects/present", headers=bob)
    r2 = client.get("/v1/buckets/sec/objects/absent", headers=bob)
    log.info("input: denied GET present->%s missing->%s",
             r1.json()["error"]["code"], r2.json()["error"]["code"])
    # request_id legitimately differs; code/message/status must be identical
    b1, b2 = r1.json()["error"], r2.json()["error"]
    assert r1.status_code == r2.status_code == 403
    assert b1["code"] == b2["code"] == "AccessDenied"
    assert b1["message"] == b2["message"]


def test_policy_per_action_over_http(client: TestClient):
    alice = {"X-OSG-Principal": "alice"}
    bob = {"X-OSG-Principal": "bob"}
    client.put("/v1/buckets/sec", headers=alice)
    client.put("/v1/buckets/sec/objects/k", content=b"v", headers=alice)
    pol = BucketPolicy(statements=[PolicyStatement(
        effect="allow", principal="bob", actions=["s3:GetObject"])])
    client.put("/v1/buckets/sec/policy", json=pol.model_dump(), headers=alice)

    assert client.get("/v1/buckets/sec/objects/k", headers=bob).status_code == 200
    assert client.get("/v1/buckets/sec/objects",
                      headers=bob).status_code == 403
    assert client.put("/v1/buckets/sec/objects/k2", content=b"x",
                      headers=bob).status_code == 403
    assert client.delete("/v1/buckets/sec/objects/k",
                         headers=bob).status_code == 403
