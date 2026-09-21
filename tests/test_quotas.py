"""Quota and limit enforcement: rejections are explicit and leave no residue."""

from tests.conftest import ALICE


def test_size_quota_rejection_is_clean(make_client):
    client, settings = make_client(quota_bytes=10)
    client.put("/buckets/b", headers=ALICE)
    assert client.put("/buckets/b/objects/a", headers=ALICE, content=b"12345").status_code == 200
    print("\n[quota] 10 bytes total, 5 used")
    r = client.put("/buckets/b/objects/b", headers=ALICE, content=b"123456")
    print(f"  write 6 more bytes -> {r.status_code} {r.json()['error']['code']}")
    assert r.status_code == 403 and r.json()["error"]["code"] == "QuotaExceeded"
    print("[case] rejected write leaves no object, no tmp file, usage unchanged")
    assert client.get("/buckets/b/objects/b", headers=ALICE).status_code == 404
    assert list((settings.data_dir / "buckets" / "b" / "objects-data").glob("*.tmp")) == []
    print("[case] overwrite within quota still works (delta accounting)")
    assert client.put("/buckets/b/objects/a", headers=ALICE, content=b"1234567890").status_code == 200
    r = client.put("/buckets/b/objects/a", headers=ALICE, content=b"12345678901")
    assert r.status_code == 403  # 11 > 10


def test_max_object_size(make_client):
    client, settings = make_client(max_object_size=8)
    client.put("/buckets/b", headers=ALICE)
    print("\n[limit] max_object_size=8")
    r = client.put("/buckets/b/objects/big", headers=ALICE, content=b"0123456789")
    print(f"  10-byte write -> {r.status_code} {r.json()['error']['code']}")
    assert r.status_code == 413 and r.json()["error"]["code"] == "EntityTooLarge"
    assert client.get("/buckets/b/objects/big", headers=ALICE).status_code == 404
    assert list((settings.data_dir / "buckets" / "b" / "objects-data").iterdir()) == []


def test_active_upload_limit(make_client):
    client, settings = make_client(max_active_uploads=2)
    client.put("/buckets/b", headers=ALICE)
    print("\n[limit] max_active_uploads=2")
    for k in ("u1", "u2"):
        assert client.post("/buckets/b/uploads", headers=ALICE,
                           params={"key": k}).status_code == 200
    r = client.post("/buckets/b/uploads", headers=ALICE, params={"key": "u3"})
    print(f"  third upload -> {r.status_code} {r.json()['error']['code']}")
    assert r.status_code == 429 and r.json()["error"]["code"] == "TooManyRequests"
    uploads = client.get("/buckets/b/uploads", headers=ALICE).json()["uploads"]
    print(f"  active uploads still {len(uploads)}; no placeholder leaked")
    assert len(uploads) == 2
    print("[case] aborting one frees the slot")
    client.delete(f"/buckets/b/uploads/{uploads[0]['upload_id']}", headers=ALICE)
    assert client.post("/buckets/b/uploads", headers=ALICE,
                       params={"key": "u3"}).status_code == 200


def test_part_quota_and_retransmit_no_double_count(make_client):
    client, settings = make_client(quota_bytes=8)
    client.put("/buckets/b", headers=ALICE)
    uid = client.post("/buckets/b/uploads", headers=ALICE,
                      params={"key": "m"}).json()["upload"]["upload_id"]
    assert client.put(f"/buckets/b/uploads/{uid}/parts/1", headers=ALICE,
                      content=b"1234").status_code == 200
    print("\n[quota] 8 bytes, part1=4 bytes stored")
    r = client.put(f"/buckets/b/uploads/{uid}/parts/1", headers=ALICE, content=b"abcd")
    print(f"  retransmit same part (4 bytes) -> {r.status_code} (replace, not add)")
    assert r.status_code == 200
    r = client.put(f"/buckets/b/uploads/{uid}/parts/2", headers=ALICE, content=b"12345")
    print(f"  second part of 5 bytes (4+5>8) -> {r.status_code} {r.json()['error']['code']}")
    assert r.status_code == 403 and r.json()["error"]["code"] == "QuotaExceeded"
    part_dir = settings.data_dir / "buckets" / "b" / "uploads-data" / uid
    print("  files left in part dir:", sorted(p.name for p in part_dir.iterdir()))
    assert sorted(p.name for p in part_dir.iterdir()) == ["1"]
