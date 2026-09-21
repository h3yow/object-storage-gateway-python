"""End-to-end HTTP behaviour: stable envelope, codes, idempotent retries."""
from __future__ import annotations

import logging

import pytest
from fastapi.testclient import TestClient

from app.api import create_app
from app.config import load_settings

log = logging.getLogger("test.http")


@pytest.fixture
def client(tmp_path):
    settings = load_settings(str(tmp_path / "data"))
    with TestClient(create_app(settings)) as c:
        yield c


H = {"X-OSG-Principal": "alice"}


def test_error_envelope_is_stable(client: TestClient):
    r = client.get("/v1/buckets/nope/objects/k", headers=H)
    assert r.status_code == 404
    body = r.json()
    log.info("input: GET missing bucket -> %s", body)
    assert set(body["error"]) == {"code", "message", "request_id"}
    assert body["error"]["code"] == "NoSuchBucket"
    # missing identity: distinct 401 envelope
    r = client.get("/v1/buckets/nope/objects/k")
    assert r.status_code == 401
    assert r.json()["error"]["code"] == "Unauthorized"


def test_full_lifecycle_over_http(client: TestClient):
    assert client.put("/v1/buckets/bk", headers=H).status_code == 201
    r = client.put("/v1/buckets/bk/objects/a", content=b"hello",
                   headers={**H, "Content-Type": "text/plain"})
    assert r.status_code == 200
    etag = r.json()["etag"]
    assert r.headers["ETag"] == f'"{etag}"'

    r = client.get("/v1/buckets/bk/objects/a", headers=H)
    assert r.status_code == 200
    assert r.content == b"hello"
    assert r.headers["X-OSG-Version-Id"]

    # conditional overwrite: stale etag -> 412
    r = client.put("/v1/buckets/bk/objects/a", content=b"v2",
                   headers={**H, "If-Match": '"0"*64'})
    assert r.status_code == 412
    assert r.json()["error"]["code"] == "PreconditionFailed"
    r = client.put("/v1/buckets/bk/objects/a", content=b"v2",
                   headers={**H, "If-Match": f'"{etag}"'})
    assert r.status_code == 200

    # delete is idempotent: both calls 200, only first "existed"
    d1 = client.delete("/v1/buckets/bk/objects/a", headers=H)
    d2 = client.delete("/v1/buckets/bk/objects/a", headers=H)
    assert d1.json()["existed"] is True
    assert d2.json()["existed"] is False
    assert client.get("/v1/buckets/bk/objects/a",
                      headers=H).status_code == 404


def test_multipart_http_flow_and_retries(client: TestClient):
    client.put("/v1/buckets/bk", headers=H)
    r = client.post("/v1/buckets/bk/objects/big?uploads", headers=H)
    uid = r.json()["upload_id"]
    e = {}
    for n, payload in [(2, b"BBBB"), (1, b"AAAA"), (3, b"CC")]:
        rr = client.put(
            f"/v1/buckets/bk/objects/big/parts/{n}?uploadId={uid}",
            content=payload, headers=H)
        assert rr.status_code == 200
        e[n] = rr.json()["etag"]
    # retry part 1 identically — same etag, no duplication
    r1again = client.put(
        f"/v1/buckets/bk/objects/big/parts/1?uploadId={uid}",
        content=b"AAAA", headers=H)
    assert r1again.json()["etag"] == e[1]

    manifest = {"parts": [{"part_number": n, "etag": e[n]} for n in (1, 2, 3)]}
    r = client.post(f"/v1/buckets/bk/objects/big?uploadId={uid}",
                    json=manifest, headers=H)
    assert r.status_code == 200
    version = r.json()["version_id"]
    # timeout-retry of complete returns the same version, no new object
    r2 = client.post(f"/v1/buckets/bk/objects/big?uploadId={uid}",
                     json=manifest, headers=H)
    assert r2.status_code == 200
    assert r2.json()["version_id"] == version
    assert client.get("/v1/buckets/bk/objects/big",
                      headers=H).content == b"AAAABBBBCC"
    # upload now gone
    r3 = client.post(f"/v1/buckets/bk/objects/big?uploadId={uid}"
                     f"&action=parts", headers=H)
    assert r3.status_code == 404


def test_oversized_object_rejected_without_residue(client: TestClient,
                                                    tmp_path):
    client.put("/v1/buckets/bk", headers=H)
    # 2 MiB body, gateway cap 1 MiB (fixture conftest setting)
    big = b"x" * (2 * 1024 * 1024)
    r = client.put("/v1/buckets/bk/objects/huge", content=big, headers=H)
    log.info("input: 2MiB upload under 1MiB cap -> %s %s",
             r.status_code, r.json()["error"]["code"])
    assert r.status_code == 413
    assert r.json()["error"]["code"] == "EntityTooLarge"
    data = tmp_path / "data"
    leftovers = list(data.rglob(".incoming-*")) + list(data.rglob(".*.tmp"))
    assert leftovers == []
    listed = client.get("/v1/buckets/bk/objects", headers=H).json()
    assert listed["objects"] == []
