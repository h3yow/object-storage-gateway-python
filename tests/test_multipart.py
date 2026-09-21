"""Multipart uploads: out-of-order parts, retransmission, abort, merge,
invisibility of incomplete uploads, idempotent retry behaviour."""

from tests.conftest import ALICE


def _create_upload(client, bucket="b", key="big"):
    client.put(f"/buckets/{bucket}", headers=ALICE)
    r = client.post(f"/buckets/{bucket}/uploads", headers=ALICE, params={"key": key})
    assert r.status_code == 200
    return r.json()["upload"]["upload_id"]


def test_out_of_order_and_retransmit(client):
    uid = _create_upload(client)
    print(f"\n[case] upload {uid[:8]}: parts arrive 3,1,2(stale),2(retransmit)")
    client.put(f"/buckets/b/uploads/{uid}/parts/3", headers=ALICE, content=b"CCC")
    client.put(f"/buckets/b/uploads/{uid}/parts/1", headers=ALICE, content=b"AAA")
    client.put(f"/buckets/b/uploads/{uid}/parts/2", headers=ALICE, content=b"XXX")
    r = client.put(f"/buckets/b/uploads/{uid}/parts/2", headers=ALICE, content=b"BBB")
    assert r.status_code == 200
    print("  retransmitted part 2 replaces stale copy (no double count)")

    print("[case] incomplete upload is not visible as an object")
    r = client.get("/buckets/b/objects/big", headers=ALICE)
    assert r.status_code == 404 and r.json()["error"]["code"] == "NoSuchKey"

    r = client.post(f"/buckets/b/uploads/{uid}/complete", headers=ALICE)
    assert r.status_code == 200
    obj = r.json()["object"]
    print(f"  merged size={obj['size']} etag={obj['etag'][:8]}")
    assert client.get("/buckets/b/objects/big", headers=ALICE).content == b"AAABBBCCC"

    print("[case] upload state removed after merge; retry of complete -> NoSuchUpload")
    assert client.get("/buckets/b/uploads", headers=ALICE).json()["uploads"] == []
    r = client.post(f"/buckets/b/uploads/{uid}/complete", headers=ALICE)
    print("  retried complete ->", r.status_code, r.json()["error"]["code"])
    assert r.status_code == 404 and r.json()["error"]["code"] == "NoSuchUpload"


def test_abort_leaves_no_residue(client, settings):
    uid = _create_upload(client)
    client.put(f"/buckets/b/uploads/{uid}/parts/1", headers=ALICE, content=b"DATA")
    print(f"\n[case] abort upload {uid[:8]} with one stored part")
    assert client.delete(f"/buckets/b/uploads/{uid}", headers=ALICE).status_code == 200
    r = client.get("/buckets/b/uploads", headers=ALICE)
    assert r.json()["uploads"] == []
    part_dir = settings.data_dir / "buckets" / "b" / "uploads-data" / uid
    print("  part dir exists after abort:", part_dir.exists())
    assert not part_dir.exists()
    print("[case] part upload to aborted/unknown upload -> NoSuchUpload")
    r = client.put(f"/buckets/b/uploads/{uid}/parts/1", headers=ALICE, content=b"X")
    assert r.status_code == 404 and r.json()["error"]["code"] == "NoSuchUpload"


def test_complete_with_conditional(client):
    client.put("/buckets/b", headers=ALICE)
    client.put("/buckets/b/objects/big", headers=ALICE, content=b"old")
    uid = _create_upload(client)
    client.put(f"/buckets/b/uploads/{uid}/parts/1", headers=ALICE, content=b"new")
    print("\n[case] complete with if_absent over existing key -> Conflict, upload kept")
    r = client.post(f"/buckets/b/uploads/{uid}/complete", headers=ALICE,
                    params={"if_absent": "true"})
    assert r.status_code == 409 and r.json()["error"]["code"] == "Conflict"
    assert client.get("/buckets/b/uploads", headers=ALICE).json()["uploads"] != []
    print("[case] complete with stale if_version -> PreconditionFailed")
    r = client.post(f"/buckets/b/uploads/{uid}/complete", headers=ALICE,
                    params={"if_version": 9})
    assert r.status_code == 412
    print("[case] complete with correct if_version -> merged")
    r = client.post(f"/buckets/b/uploads/{uid}/complete", headers=ALICE,
                    params={"if_version": 1})
    assert r.status_code == 200
    assert client.get("/buckets/b/objects/big", headers=ALICE).content == b"new"


def test_complete_empty_upload_rejected(client):
    uid = _create_upload(client)
    print("\n[case] complete with zero parts -> InvalidPart, upload still resumable")
    r = client.post(f"/buckets/b/uploads/{uid}/complete", headers=ALICE)
    assert r.status_code == 400 and r.json()["error"]["code"] == "InvalidPart"
    assert client.get("/buckets/b/uploads", headers=ALICE).json()["uploads"] != []
