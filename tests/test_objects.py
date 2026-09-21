"""Bucket/object lifecycle, error taxonomy, conditional writes, pagination."""

from tests.conftest import ALICE, BOB


def test_bucket_lifecycle_and_errors(client):
    print("\n[case] create bucket as alice, then duplicate create")
    assert client.put("/buckets/b1", headers=ALICE).status_code == 200
    r = client.put("/buckets/b1", headers=ALICE)
    print("  duplicate create ->", r.status_code, r.json()["error"]["code"])
    assert r.status_code == 409 and r.json()["error"]["code"] == "BucketAlreadyExists"

    print("[case] get/delete missing bucket are distinguishable 404s")
    r = client.get("/buckets/ghost", headers=ALICE)
    assert r.status_code == 404 and r.json()["error"]["code"] == "NoSuchBucket"
    r = client.delete("/buckets/ghost", headers=ALICE)
    assert r.status_code == 404

    print("[case] delete non-empty bucket -> BucketNotEmpty; empty -> ok")
    client.put("/buckets/b1/objects/k", headers=ALICE, content=b"v")
    r = client.delete("/buckets/b1", headers=ALICE)
    print("  delete non-empty ->", r.status_code, r.json()["error"]["code"])
    assert r.status_code == 409 and r.json()["error"]["code"] == "BucketNotEmpty"
    client.delete("/buckets/b1/objects/k", headers=ALICE)
    assert client.delete("/buckets/b1", headers=ALICE).status_code == 200


def test_object_crud_and_overwrite_versions(client):
    client.put("/buckets/b", headers=ALICE)
    r = client.put("/buckets/b/objects/dir/a.txt", headers=ALICE, content=b"v1")
    assert r.status_code == 200 and r.json()["object"]["version"] == 1
    print("\n[case] overwrite bumps version, content is replaced atomically")
    r = client.put("/buckets/b/objects/dir/a.txt", headers=ALICE, content=b"v2-longer")
    assert r.json()["object"]["version"] == 2
    assert client.get("/buckets/b/objects/dir/a.txt", headers=ALICE).content == b"v2-longer"

    print("[case] read missing key -> NoSuchKey; delete missing key -> NoSuchKey")
    r = client.get("/buckets/b/objects/missing", headers=ALICE)
    assert r.status_code == 404 and r.json()["error"]["code"] == "NoSuchKey"
    r = client.delete("/buckets/b/objects/missing", headers=ALICE)
    assert r.status_code == 404 and r.json()["error"]["code"] == "NoSuchKey"

    print("[case] delete is one-shot: second delete reports NoSuchKey (predictable retry)")
    assert client.delete("/buckets/b/objects/dir/a.txt", headers=ALICE).status_code == 200
    r = client.delete("/buckets/b/objects/dir/a.txt", headers=ALICE)
    assert r.status_code == 404


def test_conditional_writes(client):
    client.put("/buckets/b", headers=ALICE)
    print("\n[case] if_absent: first succeeds, second is a distinguishable conflict")
    assert client.put("/buckets/b/objects/k", headers=ALICE, content=b"1",
                      params={"if_absent": "true"}).status_code == 200
    r = client.put("/buckets/b/objects/k", headers=ALICE, content=b"2",
                   params={"if_absent": "true"})
    print("  second if_absent ->", r.status_code, r.json()["error"]["code"])
    assert r.status_code == 409 and r.json()["error"]["code"] == "Conflict"
    assert client.get("/buckets/b/objects/k", headers=ALICE).content == b"1"

    print("[case] if_version: stale writer rejected, current writer commits")
    r = client.put("/buckets/b/objects/k", headers=ALICE, content=b"2",
                   params={"if_version": 7})
    print("  stale if_version=7 (current=1) ->", r.status_code, r.json()["error"]["code"])
    assert r.status_code == 412 and r.json()["error"]["code"] == "PreconditionFailed"
    r = client.put("/buckets/b/objects/k", headers=ALICE, content=b"2",
                   params={"if_version": 1})
    assert r.status_code == 200 and r.json()["object"]["version"] == 2


def test_list_pagination_bounded(client):
    client.put("/buckets/b", headers=ALICE)
    for i in range(5):
        client.put(f"/buckets/b/objects/k{i}", headers=ALICE, content=b"x")
    print("\n[case] limit=2 paginates with marker; no unbounded result set")
    r = client.get("/buckets/b/objects", headers=ALICE, params={"limit": 2})
    page = r.json()
    print("  page1:", [o["key"] for o in page["objects"]], "next:", page["next_marker"])
    assert [o["key"] for o in page["objects"]] == ["k0", "k1"] and page["truncated"]
    r = client.get("/buckets/b/objects", headers=ALICE,
                   params={"limit": 2, "marker": page["next_marker"]})
    keys = [o["key"] for o in r.json()["objects"]]
    print("  page2:", keys)
    assert keys == ["k2", "k3"]
    print("[case] prefix filter")
    client.put("/buckets/b/objects/other", headers=ALICE, content=b"x")
    r = client.get("/buckets/b/objects", headers=ALICE, params={"prefix": "oth"})
    assert [o["key"] for o in r.json()["objects"]] == ["other"]
