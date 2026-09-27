"""Historical version retention: enable/disable paths, version listing and
reads, snapshot-consistent reads under concurrent overwrite, rollback as a
new conditional write, and retention eviction with space reclamation."""

import threading

from tests.conftest import ALICE, BOB


def _enable(client, bucket="b", **body):
    client.put(f"/buckets/{bucket}", headers=ALICE)
    body.setdefault("enabled", True)
    r = client.put(f"/buckets/{bucket}/versioning", headers=ALICE, json=body)
    assert r.status_code == 200, r.text
    return r.json()["bucket"]


def _put(client, key, body, bucket="b"):
    r = client.put(f"/buckets/{bucket}/objects/{key}", headers=ALICE, content=body)
    assert r.status_code == 200, r.text
    return r.json()["object"]


def _versions(client, key, bucket="b"):
    r = client.get(f"/buckets/{bucket}/versions/{key}", headers=ALICE)
    assert r.status_code == 200, r.text
    return r.json()["versions"]


def test_disabled_bucket_behaves_unchanged(client):
    client.put("/buckets/b", headers=ALICE)
    print("\n[case] versioning off: overwrite destroys old content, no versions")
    _put(client, "k", b"v1")
    _put(client, "k", b"v2")
    assert client.get("/buckets/b/objects/k", headers=ALICE).content == b"v2"
    r = client.get("/buckets/b/versions/k", headers=ALICE)
    print("  list versions on disabled bucket ->", r.status_code)
    assert r.status_code == 200
    vs = r.json()["versions"]
    assert len(vs) == 1 and vs[0]["is_current"] and vs[0]["size"] == 2
    assert vs[0]["version_id"] == ""  # legacy body has no version id
    # no version-storage directories carry data for this key
    data = client.app.state.settings.data_dir / "buckets" / "b" / "versions-data"
    assert not any(data.iterdir())
    print("[case] reading a fabricated version id -> 404 NoSuchVersion")
    r = client.get(f"/buckets/b/version/{'ab'*16}/k", headers=ALICE)
    assert r.status_code == 404 and r.json()["error"]["code"] == "NoSuchVersion"
    print("[case] rollback on a disabled bucket -> 400 InvalidRequest")
    r = client.post(f"/buckets/b/version/{'ab'*16}/k/rollback", headers=ALICE)
    assert r.status_code == 400 and r.json()["error"]["code"] == "InvalidRequest"


def test_enabled_bucket_keeps_overwrite_history(client):
    client.put("/buckets/b", headers=ALICE)
    o1 = _put(client, "k", b"v1")          # written before enabling (legacy body)
    assert o1["version_id"] == ""
    _enable(client, enabled=True)
    print("\n[case] enabled: each overwrite yields a stable version id; "
          "the pre-existing legacy body is migrated into version storage")
    o2 = _put(client, "k", b"v2-longer")
    o3 = _put(client, "k", b"v3")
    assert o2["version_id"] and o3["version_id"] != o2["version_id"]

    vs = _versions(client, "k")
    print("  versions:", [(v["version"], v["is_current"]) for v in vs])
    assert [v["version"] for v in vs] == [3, 2, 1]
    assert [v["is_current"] for v in vs] == [True, False, False]

    print("[case] read each version by its stable id; content matches header")
    for v in vs:
        r = client.get(f"/buckets/b/version/{v['version_id']}/k", headers=ALICE)
        assert r.status_code == 200
        assert int(r.headers["Content-Length"]) == len(r.content) == v["size"]
        assert r.headers["X-ETag"] == v["etag"]
    v2 = next(v for v in vs if v["version"] == 2)
    assert client.get(f"/buckets/b/version/{v2['version_id']}/k",
                      headers=ALICE).content == b"v2-longer"
    # migrated legacy body is readable as version 1
    v1 = next(v for v in vs if v["version"] == 1)
    assert client.get(f"/buckets/b/version/{v1['version_id']}/k",
                      headers=ALICE).content == b"v1"
    # current read still works and advertises the version id
    r = client.get("/buckets/b/objects/k", headers=ALICE)
    assert r.content == b"v3" and r.headers["X-Version-Id"] == o3["version_id"]


def test_multipart_merge_is_versioned(client):
    _enable(client, enabled=True)
    _put(client, "big", b"old")
    uid = client.post("/buckets/b/uploads", headers=ALICE,
                      params={"key": "big"}).json()["upload"]["upload_id"]
    client.put(f"/buckets/b/uploads/{uid}/parts/1", headers=ALICE, content=b"AAA")
    client.put(f"/buckets/b/uploads/{uid}/parts/2", headers=ALICE, content=b"BBB")
    print("\n[case] multipart complete archives the pre-merge content")
    r = client.post(f"/buckets/b/uploads/{uid}/complete", headers=ALICE)
    assert r.status_code == 200
    assert client.get("/buckets/b/objects/big", headers=ALICE).content == b"AAABBB"
    vs = _versions(client, "big")
    assert [v["version"] for v in vs] == [2, 1]
    old = vs[-1]
    r = client.get(f"/buckets/b/version/{old['version_id']}/big", headers=ALICE)
    assert r.content == b"old"


def test_version_read_self_consistent_under_concurrent_overwrite(client):
    _enable(client, enabled=True)
    _put(client, "hot", b"x" * 100)
    # create several history versions, all retained (no limit)
    ids = []
    for i in range(1, 8):
        ids.append(_put(client, "hot", (f"v{i}-" + "y" * 200).encode())["version_id"])

    print("\n[case] readers pin a version while writers overwrite + evict nothing")
    stop = threading.Event()
    errors = []

    def writer(i):
        j = 0
        while not stop.is_set():
            try:
                _put(client, "hot", f"w{i}-{j}-".encode() + b"z" * 300)
            except Exception as e:  # noqa: BLE001
                errors.append(e)
            j += 1

    def reader():
        import time
        deadline = time.time() + 1.5
        while time.time() < deadline:
            vid = ids[(int(time.time() * 1000)) % len(ids)]
            r = client.get(f"/buckets/b/version/{vid}/hot", headers=ALICE)
            if r.status_code != 200:
                errors.append(AssertionError(f"read {r.status_code}"))
                return
            # header metadata and body must be one and the same version
            if int(r.headers["Content-Length"]) != len(r.content):
                errors.append(AssertionError("length/header mismatch"))
            import hashlib
            if hashlib.sha256(r.content).hexdigest() != r.headers["X-ETag"]:
                errors.append(AssertionError("etag/body mismatch"))

    threads = [threading.Thread(target=writer, args=(i,)) for i in range(3)]
    threads += [threading.Thread(target=reader) for _ in range(3)]
    for t in threads:
        t.start()
    import time
    time.sleep(2.0)
    stop.set()
    for t in threads:
        t.join()
    assert not errors, errors[:3]
    print("  all pinned reads returned matching length+etag+body")


def test_rollback_is_a_new_conditional_write(client):
    _enable(client, enabled=True)
    _put(client, "k", b"v1")
    _put(client, "k", b"v2")
    o3 = _put(client, "k", b"v3")
    vs = _versions(client, "k")
    v1 = next(v for v in vs if v["version"] == 1)
    print("\n[case] rollback re-submits history as a new current version")
    r = client.post(f"/buckets/b/version/{v1['version_id']}/k/rollback", headers=ALICE)
    assert r.status_code == 200
    o4 = r.json()["object"]
    assert o4["version"] == 4 and o4["version_id"] != v1["version_id"]
    assert client.get("/buckets/b/objects/k", headers=ALICE).content == b"v1"
    # source historical version is untouched and still readable
    assert client.get(f"/buckets/b/version/{v1['version_id']}/k",
                      headers=ALICE).content == b"v1"

    print("[case] rollback obeys if_version: stale -> 412, matching -> 200")
    r = client.post(f"/buckets/b/version/{v1['version_id']}/k/rollback",
                    headers=ALICE, params={"if_version": o3["version"]})
    assert r.status_code == 412 and r.json()["error"]["code"] == "PreconditionFailed"
    r = client.post(f"/buckets/b/version/{v1['version_id']}/k/rollback",
                    headers=ALICE, params={"if_version": 4})
    assert r.status_code == 200 and r.json()["object"]["version"] == 5

    print("[case] rollback of an evicted/unknown id -> 404 NoSuchVersion")
    r = client.post(f"/buckets/b/version/{'cd'*16}/k/rollback", headers=ALICE)
    assert r.status_code == 404 and r.json()["error"]["code"] == "NoSuchVersion"


def test_eviction_by_count_releases_space(client):
    _enable(client, max_versions=3)
    svc = client.app.state.service
    print("\n[case] max_versions=3: only current + 2 newest survive")
    for i in range(6):
        _put(client, "k", f"body{i}".encode())  # 6 bytes each
    vs = _versions(client, "k")
    assert [v["version"] for v in vs] == [6, 5, 4]
    assert svc.store.usage_bytes() == 15  # exactly 3 retained 5-byte bodies
    print("  surviving versions:", [v["version"] for v in vs],
          "usage:", svc.store.usage_bytes())
    print("[case] evicted ids are distinguishable 'not found'")
    # version 1's id is unknown to the service now
    r = client.get(f"/buckets/b/version/{'00'*16}/k", headers=ALICE)
    assert r.status_code == 404 and r.json()["error"]["code"] == "NoSuchVersion"
    # no dangling files on disk: metadata and data dirs agree
    root = svc.store.settings.data_dir / "buckets" / "b"
    metas = {p.stem for p in (root / "versions").rglob("*.json")}
    datas = {p.name for p in (root / "versions-data").rglob("*") if p.is_file()}
    assert metas == datas and len(metas) == 3


def test_eviction_by_bytes_and_never_current(client):
    _enable(client, max_version_bytes=10)
    print("\n[case] max_version_bytes=10 caps *historical* bytes; current exempt")
    _put(client, "k", b"aaaaaaaa")        # v1 current (8)
    _put(client, "k", b"bbbb")            # v2 current; history v1=8 <= 10 -> kept
    vs = _versions(client, "k")
    assert [v["version"] for v in vs] == [2, 1]
    _put(client, "k", b"cc")              # history 8+4=12 > 10 -> evict oldest (v1)
    vs = _versions(client, "k")
    assert [v["version"] for v in vs] == [3, 2]
    print("[case] shrinking limits later evicts on the next write, current safe")
    client.put("/buckets/b/versioning", headers=ALICE,
               json={"enabled": True, "max_version_bytes": 1})
    _put(client, "k", b"dd")
    vs = _versions(client, "k")
    assert [v["version"] for v in vs] == [4]  # v2(4B), v3(2B) history both evicted


def test_delete_removes_all_versions(client, settings):
    _enable(client, enabled=True)
    for i in range(4):
        _put(client, "k", f"v{i}".encode())
    print("\n[case] delete wipes current + history and frees all space")
    r = client.delete("/buckets/b/objects/k", headers=ALICE)
    assert r.status_code == 200
    r = client.get("/buckets/b/versions/k", headers=ALICE)
    assert r.status_code == 404 and r.json()["error"]["code"] == "NoSuchKey"
    root = settings.data_dir / "buckets" / "b"
    assert list((root / "versions").iterdir()) == []
    assert list((root / "versions-data").iterdir()) == []
    assert client.app.state.service.store.usage_bytes() == 0
    # bucket can now be deleted (empty)
    assert client.delete("/buckets/b", headers=ALICE).status_code == 200


def test_versioning_config_access_control(client):
    client.put("/buckets/b", headers=ALICE)
    print("\n[case] non-owner cannot toggle versioning")
    r = client.put("/buckets/b/versioning", headers=BOB, json={"enabled": True})
    assert r.status_code == 403 and r.json()["error"]["code"] == "AccessDenied"
    r = client.put("/buckets/b/versioning", headers=ALICE,
                   json={"enabled": True, "max_versions": 1})
    assert r.status_code == 400 and r.json()["error"]["code"] == "InvalidRequest"


def test_pinned_reads_racing_eviction_are_all_or_nothing(client):
    import collections
    _enable(client, enabled=True, max_versions=4)
    svc = client.app.state.service
    _put(client, "hot", b"init")
    recent = collections.deque(maxlen=12)
    stop = threading.Event()
    errors = []

    def writer(i):
        j = 0
        while not stop.is_set():
            o = _put(client, "hot", f"w{i}-{j:04d}-".encode() + b"q" * 64)
            recent.append((o["version_id"], o["etag"], o["size"]))
            j += 1

    def reader():
        import hashlib
        import time
        deadline = time.time() + 1.5
        while time.time() < deadline:
            if not recent:
                continue
            vid, etag, size = recent[(int(time.time() * 1000)) % len(recent)]
            r = client.get(f"/buckets/b/version/{vid}/hot", headers=ALICE)
            if r.status_code == 404:
                if r.json()["error"]["code"] != "NoSuchVersion":
                    errors.append(AssertionError("wrong 404 code"))
            elif r.status_code == 200:
                if (int(r.headers["Content-Length"]) != len(r.content)
                        or hashlib.sha256(r.content).hexdigest() != etag
                        or r.headers["X-ETag"] != etag or len(r.content) != size):
                    errors.append(AssertionError("evicted/overwritten mid-read mismatch"))
            else:
                errors.append(AssertionError(f"unexpected {r.status_code}"))

    threads = [threading.Thread(target=writer, args=(i,)) for i in range(3)]
    threads += [threading.Thread(target=reader) for _ in range(3)]
    for t in threads:
        t.start()
    import time
    time.sleep(2.0)
    stop.set()
    for t in threads:
        t.join()
    print("\n[case] pinned reads racing eviction: only 200-consistent or 404 seen, "
          "usage =", svc.store.usage_bytes())
    assert not errors, errors[:3]
    # current + at most 3 histories retained
    assert len(_versions(client, "hot")) <= 4


def test_disable_then_overwrite_and_delete_leaves_no_residue(client, settings):
    _enable(client, enabled=True)
    _put(client, "k", b"v1")
    _put(client, "k", b"v2")
    print("\n[case] disable versioning, overwrite (frozen history), then delete")
    client.put("/buckets/b/versioning", headers=ALICE, json={"enabled": False})
    _put(client, "k", b"v3-legacy")  # goes back to the legacy data location
    svc = client.app.state.service
    before = svc.store.usage_bytes()
    assert before == len(b"v3-legacy") + len(b"v1") + len(b"v2")
    r = client.delete("/buckets/b/objects/k", headers=ALICE)
    assert r.status_code == 200
    bdir = settings.data_dir / "buckets" / "b"
    assert list((bdir / "versions").iterdir()) == []
    assert list((bdir / "versions-data").iterdir()) == []
    assert svc.store.usage_bytes() == 0
    print("  all current + frozen historical bytes released")
    # and a restart reports nothing to clean
    from fastapi.testclient import TestClient
    from main import create_app
    c2, report = TestClient(create_app(settings)), None
    report = c2.app.state.recovery_report
    assert report["orphan_data_removed"] == 0
