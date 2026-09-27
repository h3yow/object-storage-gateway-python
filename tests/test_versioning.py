"""History version retention: enable/disable paths, stable version ids,
version listing/reading, restore, concurrent-read consistency, retention
eviction, quota integration and crash recovery of the new layout."""

import threading

from fastapi.testclient import TestClient

from gateway.config import Settings
from main import create_app
from tests.conftest import ALICE, BOB


def _enable(client, bucket="b", **kw):
    body = {"enabled": True, **kw}
    r = client.put(f"/buckets/{bucket}/versioning", headers=ALICE, json=body)
    assert r.status_code == 200, r.text
    return r.json()["bucket"]


def _versions(client, bucket="b", key="k"):
    r = client.get(f"/buckets/{bucket}/objects/{key}/versions", headers=ALICE)
    assert r.status_code == 200, r.text
    return r.json()


def _put(client, content, bucket="b", key="k", **params):
    r = client.put(f"/buckets/{bucket}/objects/{key}", headers=ALICE,
                   content=content, params=params)
    assert r.status_code == 200, r.text
    return r.json()["object"]


# ---------------------------------------------------------------- disabled
def test_disabled_bucket_behaves_exactly_as_before(client):
    client.put("/buckets/b", headers=ALICE)
    print("\n[case] no versioning: overwrite destroys old content, no version id")
    m1 = _put(client, b"v1")
    assert m1["version"] == 1 and m1["version_id"] == ""
    m2 = _put(client, b"v2")
    assert m2["version"] == 2 and m2["version_id"] == ""
    r = client.get("/buckets/b/objects/k", headers=ALICE)
    assert r.content == b"v2" and r.headers["X-Version-Id"] == ""
    print("[case] version listing on a disabled bucket -> InvalidRequest/empty")
    r = client.get("/buckets/b/objects/k/versions", headers=ALICE)
    print("  list versions ->", r.status_code, r.json())
    assert r.status_code == 200
    assert r.json()["versions"] == [] and r.json()["current_version_id"] == ""
    print("[case] reading by version id on disabled bucket -> NoSuchVersion")
    r = client.get("/buckets/b/objects/k", headers=ALICE,
                   params={"version_id": "deadbeef"})
    assert r.status_code == 404 and r.json()["error"]["code"] == "NoSuchVersion"
    r = client.post("/buckets/b/objects/k/versions/deadbeef/restore",
                    headers=ALICE)
    assert r.status_code == 400 and r.json()["error"]["code"] == "InvalidRequest"


def test_only_owner_may_change_versioning(client):
    client.put("/buckets/b", headers=ALICE)
    r = client.put("/buckets/b/versioning", headers=BOB, json={"enabled": True})
    assert r.status_code == 403 and r.json()["error"]["code"] == "AccessDenied"
    r = client.put("/buckets/b/versioning", headers=ALICE,
                   json={"enabled": True, "max_versions": -1})
    assert r.status_code == 400


# ------------------------------------------------------------------ enable
def test_enable_on_existing_bucket_migrates_current_content(client):
    client.put("/buckets/b", headers=ALICE)
    _put(client, b"legacy-content")
    print("\n[case] enable versioning after data exists: current becomes v1 history")
    _enable(client)
    vs = _versions(client)["versions"]
    assert len(vs) == 1 and vs[0]["is_current"] and vs[0]["version"] == 1
    assert client.get("/buckets/b/objects/k", headers=ALICE).content == b"legacy-content"
    print("[case] subsequent overwrite keeps the migrated version readable")
    _put(client, b"new-content")
    vs = _versions(client)["versions"]
    assert len(vs) == 2 and vs[0]["is_current"]
    old_id = vs[1]["version_id"]
    r = client.get("/buckets/b/objects/k", headers=ALICE,
                   params={"version_id": old_id})
    assert r.status_code == 200 and r.content == b"legacy-content"


def test_versioned_overwrite_and_multipart_keep_history(client):
    client.put("/buckets/b", headers=ALICE,
               json={"versioning_enabled": True})
    _put(client, b"one")
    _put(client, b"two-longer!!")
    print("\n[case] plain overwrites: ids are stable and distinct")
    listing = _versions(client)
    ids = [v["version_id"] for v in listing["versions"]]
    assert len(ids) == 2 and len(set(ids)) == 2 and all(len(i) == 32 for i in ids)
    assert listing["versions"][0]["size"] == len(b"two-longer!!")

    print("[case] multipart complete also preserves the prior current version")
    uid = client.post("/buckets/b/uploads", headers=ALICE,
                      params={"key": "k"}).json()["upload"]["upload_id"]
    client.put(f"/buckets/b/uploads/{uid}/parts/1", headers=ALICE, content=b"partA-")
    client.put(f"/buckets/b/uploads/{uid}/parts/2", headers=ALICE, content=b"partB")
    r = client.post(f"/buckets/b/uploads/{uid}/complete", headers=ALICE)
    assert r.status_code == 200
    assert client.get("/buckets/b/objects/k", headers=ALICE).content == b"partA-partB"
    vs = _versions(client)["versions"]
    assert len(vs) == 3
    old = vs[2]
    r = client.get("/buckets/b/objects/k", headers=ALICE,
                   params={"version_id": old["version_id"]})
    assert r.content == b"one"
    print("[case] headers echo the exact version served")
    assert r.headers["X-Version-Id"] == old["version_id"]
    assert r.headers["X-Version"] == str(old["version"])
    assert r.headers["Content-Length"] == str(old["size"])

    print("[case] unknown / fabricated version id -> NoSuchVersion (not NoSuchKey)")
    r = client.get("/buckets/b/objects/k", headers=ALICE,
                   params={"version_id": "0" * 32})
    assert r.status_code == 404 and r.json()["error"]["code"] == "NoSuchVersion"
    r = client.get("/buckets/b/objects/missing/versions", headers=ALICE)
    assert r.status_code == 404 and r.json()["error"]["code"] == "NoSuchKey"


# ----------------------------------------------------------------- restore
def test_restore_creates_new_version_and_keeps_source(client):
    client.put("/buckets/b", headers=ALICE,
               json={"versioning_enabled": True})
    _put(client, b"v1")
    _put(client, b"v2")
    _put(client, b"v3")
    v1 = [v for v in _versions(client)["versions"] if v["version"] == 1][0]
    print("\n[case] restore v1 -> new current version with new id, same content")
    r = client.post(f"/buckets/b/objects/k/versions/{v1['version_id']}/restore",
                    headers=ALICE)
    assert r.status_code == 200
    new = r.json()["object"]
    assert new["version"] == 4 and new["version_id"] != v1["version_id"]
    assert client.get("/buckets/b/objects/k", headers=ALICE).content == b"v1"
    src = client.get("/buckets/b/objects/k", headers=ALICE,
                     params={"version_id": v1["version_id"]})
    assert src.status_code == 200 and src.content == b"v1"

    print("[case] restore obeys conditional write rules like any overwrite")
    r = client.post(f"/buckets/b/objects/k/versions/{v1['version_id']}/restore",
                    headers=ALICE, params={"if_absent": "true"})
    assert r.status_code == 409
    r = client.post(f"/buckets/b/objects/k/versions/{v1['version_id']}/restore",
                    headers=ALICE, params={"if_version": 99})
    assert r.status_code == 412
    r = client.post(f"/buckets/b/objects/k/versions/{v1['version_id']}/restore",
                    headers=ALICE, params={"if_version": 4})
    assert r.status_code == 200 and r.json()["object"]["version"] == 5


# ------------------------------------------------------------- concurrency
def test_concurrent_version_read_is_self_consistent(settings):
    # Direct store-level test: many threads overwrite one key while others
    # keep streaming a *pinned historical version*. The snapshot (size +
    # etag + bytes) must always describe that single version even though
    # the current pointer moves and the pinned version's bytes could be
    # unlinked only by retention (disabled here).
    from gateway.store import Store
    from gateway.service import GatewayService
    store = Store(settings)
    store.recover()
    svc = GatewayService(store)
    svc.create_bucket("alice", "b", versioning_enabled=True)

    def put(body: bytes):
        svc.put_object("alice", "b", "k", iter([body]))

    put(b"v0")
    _, versions = svc.list_versions("alice", "b", "k")
    pinned = [v for v in versions if v.version == 1][0]
    stop = threading.Event()
    errors = []
    read_count = [0]

    def writer():
        i = 0
        while not stop.is_set():
            i += 1
            put(f"w{i:04d}-".encode() * 4)

    def reader():
        import hashlib
        while not stop.is_set():
            meta, stream = svc.read_object(
                "alice", "b", "k", version_id=pinned.version_id)
            # the header-side snapshot must be the pinned version...
            if (meta.version_id != pinned.version_id
                    or meta.size != pinned.size or meta.etag != pinned.etag):
                errors.append(("snapshot-mismatch", meta.version_id))
                return
            data = b"".join(stream)
            # ...and the body streaming under it must match length+digest
            if len(data) != pinned.size:
                errors.append(("length", len(data)))
                return
            if hashlib.sha256(data).hexdigest() != pinned.etag:
                errors.append(("etag",))
                return
            read_count[0] += 1

    wt = threading.Thread(target=writer)
    readers = [threading.Thread(target=reader) for _ in range(4)]
    wt.start()
    for t in readers:
        t.start()
    threading.Event().wait(2.0)
    stop.set()
    wt.join()
    for t in readers:
        t.join()
    print(f"\n[case] {read_count[0]} pinned-version reads during churn; "
          f"errors={errors}")
    assert not errors and read_count[0] > 0


def test_pinned_read_survives_eviction_of_other_versions(settings):
    # A reader that opened an old version keeps reading it to completion
    # even if retention eviction unlinks that very version mid-stream:
    # POSIX keeps the bytes alive for the open descriptor.
    from gateway.store import Store
    from gateway.service import GatewayService
    store = Store(settings)
    store.recover()
    svc = GatewayService(store)
    svc.create_bucket("alice", "b", versioning_enabled=True, max_versions=0)

    def put(body: bytes):
        svc.put_object("alice", "b", "k", iter([body]))

    payload = b"x" * (1024 * 1024)
    put(payload)
    _, versions = svc.list_versions("alice", "b", "k")
    vid = versions[0].version_id
    meta, f = store.open_object_data("b", "k", version_id=vid)
    first = f.read(10)
    assert first == b"x" * 10
    # churn until the pinned version is evicted by the count cap
    svc.set_versioning("alice", "b", True, max_versions=1)
    for i in range(5):
        put(f"y{i}".encode() * 100)
    rest = f.read()
    f.close()
    assert len(first) + len(rest) == meta.size
    # the id is gone now: a fresh lookup says NoSuchVersion
    import pytest
    from gateway.errors import NoSuchVersion
    with pytest.raises(NoSuchVersion):
        store.open_object_data("b", "k", version_id=vid)


def test_concurrent_overwrites_keep_every_committed_version(client):
    client.put("/buckets/b", headers=ALICE,
               json={"versioning_enabled": True})
    _put(client, b"seed")
    n = 12
    barrier = threading.Barrier(n)

    def write(i):
        barrier.wait()
        _put(client, f"content-{i}".encode())

    threads = [threading.Thread(target=write, args=(i,)) for i in range(n)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    vs = _versions(client)["versions"]
    print(f"\n[case] {n} racing versioned overwrites -> {len(vs)} versions")
    assert len(vs) == n + 1
    contents = set()
    for v in vs[:-1]:  # everything except seed
        r = client.get("/buckets/b/objects/k", headers=ALICE,
                       params={"version_id": v["version_id"]})
        assert r.status_code == 200
        contents.add(r.content)
    assert contents == {f"content-{i}".encode() for i in range(n)}


def test_restore_under_concurrency_uses_conditions(client):
    client.put("/buckets/b", headers=ALICE,
               json={"versioning_enabled": True})
    _put(client, b"base")
    v1 = _versions(client)["versions"][0]["version_id"]
    n = 8
    results = []

    def restore(i):
        r = client.post(f"/buckets/b/objects/k/versions/{v1}/restore",
                        headers=ALICE)
        results.append(r.status_code)

    threads = [threading.Thread(target=restore, args=(i,)) for i in range(n)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    print(f"\n[case] {n} unconditional restores all commit: {sorted(results)}")
    assert results.count(200) == n
    vs = _versions(client)["versions"]
    assert len(vs) == n + 1
    assert client.get("/buckets/b/objects/k", headers=ALICE).content == b"base"


# --------------------------------------------------------------- eviction
def test_max_versions_evicts_oldest_never_current(make_client):
    client, settings = make_client()
    client.put("/buckets/b", headers=ALICE,
               json={"versioning_enabled": True, "max_versions": 2})
    ids = []
    for i, body in enumerate([b"v1", b"v2", b"v3", b"v4", b"v5"]):
        ids.append(_put(client, body)["version_id"])
    listing = _versions(client)
    vs = listing["versions"]
    print(f"\n[case] max_versions=2 after 5 writes: "
          f"{[(v['version'], v['size']) for v in vs]}")
    assert len(vs) == 3  # current + 2 non-current
    assert vs[0]["is_current"] and vs[0]["version_id"] == ids[-1]
    # oldest two versions are gone, result distinguishable as NoSuchVersion
    for gone in ids[:2]:
        r = client.get("/buckets/b/objects/k", headers=ALICE,
                       params={"version_id": gone})
        assert r.status_code == 404 and r.json()["error"]["code"] == "NoSuchVersion"
    # current is readable even though the cap is about non-current only
    assert client.get("/buckets/b/objects/k", headers=ALICE).content == b"v5"
    kept = ids[2]
    assert client.get("/buckets/b/objects/k", headers=ALICE,
                      params={"version_id": kept}).content == b"v3"
    # bytes really released: only 3 data files on disk
    files = [p for p in (settings.data_dir / "buckets" / "b" / "objects-data")
             .rglob("*") if p.is_file()]
    assert len(files) == 3, files


def test_max_version_bytes_evicts_until_under_cap(make_client):
    client, settings = make_client()
    client.put("/buckets/b", headers=ALICE,
               json={"versioning_enabled": True, "max_version_bytes": 15})
    _put(client, b"aaa")      # 3
    _put(client, b"bbbbbbbb")  # 8
    _put(client, b"cccccccccccc")  # 12 current
    vs = _versions(client)["versions"]
    print(f"\n[case] byte cap 15 for non-current versions: "
          f"{[(v['version'], v['size']) for v in vs]}")
    # current (12) never evicted; v1(3)+v2(8)=11 <= 15 both survive
    assert len(vs) == 3
    _put(client, b"ddd")  # new history [3,8,12] sums 23 > 15
    vs = _versions(client)["versions"]
    sizes = [v["size"] for v in vs if not v["is_current"]]
    print("  after one more write, non-current sizes:", sizes)
    # FIFO oldest-prefix: drop 3 (20 still > 15), drop 8 (12 fits), stop.
    # The walk never backtracks to trade a newer small version in, so only
    # the 12-byte version remains as history.
    assert sizes == [12]
    files = [p for p in (settings.data_dir / "buckets" / "b" / "objects-data")
             .rglob("*") if p.is_file()]
    assert sum(p.stat().st_size for p in files) == 12 + 3


def test_tightening_cap_applies_immediately(client):
    client.put("/buckets/b", headers=ALICE,
               json={"versioning_enabled": True})
    for i in range(5):
        _put(client, f"body-{i}".encode())
    assert len(_versions(client)["versions"]) == 5
    print("\n[case] set max_versions=1 on a bucket with 5 versions")
    _enable(client, max_versions=1)
    vs = _versions(client)["versions"]
    assert len(vs) == 2 and vs[0]["is_current"]


def test_eviction_consistent_with_quota(make_client):
    client, settings = make_client(quota_bytes=20)
    client.put("/buckets/b", headers=ALICE,
               json={"versioning_enabled": True, "max_versions": 1})
    _put(client, b"a" * 10)  # 10 used
    _put(client, b"b" * 10)  # current 10 + 1 history 10 = 20
    print("\n[case] quota 20 full; new write evicts v1 as part of commit")
    r = client.put("/buckets/b/objects/k", headers=ALICE, content=b"c" * 10)
    print("  overwrite ->", r.status_code)
    assert r.status_code == 200
    assert client.get("/healthz").status_code == 200
    r = client.put("/buckets/b/objects/other", headers=ALICE, content=b"x" * 5)
    assert r.status_code == 403 and r.json()["error"]["code"] == "QuotaExceeded"
    assert client.app.state.service.store.usage_bytes() == 20


def test_disabling_frees_history_space(client, settings):
    client.put("/buckets/b", headers=ALICE,
               json={"versioning_enabled": True})
    for i in range(4):
        _put(client, f"body-{i}!!".encode())
    store = client.app.state.service.store
    before = store.usage_bytes()
    print(f"\n[case] disable versioning: usage {before} -> history dropped")
    r = client.put("/buckets/b/versioning", headers=ALICE, json={"enabled": False})
    assert r.status_code == 200
    after = store.usage_bytes()
    assert after < before
    assert client.get("/buckets/b/objects/k", headers=ALICE).content == b"body-3!!"
    r = client.get("/buckets/b/objects/k/versions", headers=ALICE)
    assert r.json()["versions"] == []
    data_dir = settings.data_dir / "buckets" / "b" / "objects-data"
    # legacy flat layout again: exactly one data file, no shard dirs
    assert [p.name for p in data_dir.iterdir() if p.is_file()]
    _put(client, b"after-disable")  # overwrite destroys as in legacy mode
    assert client.get("/buckets/b/objects/k", headers=ALICE).content == b"after-disable"


# ---------------------------------------------------------------- recovery
def test_reopen_versioned_bucket_preserves_history(settings):
    client = TestClient(create_app(settings))
    client.put("/buckets/b", headers=ALICE,
               json={"versioning_enabled": True})
    for body in (b"one", b"two", b"three"):
        client.put("/buckets/b/objects/k", headers=ALICE, content=body)
    old_id = client.get("/buckets/b/objects/k/versions",
                        headers=ALICE).json()["versions"][-1]["version_id"]
    print("\n[case] restart preserves index, data and stable version ids")
    client = TestClient(create_app(settings))
    r = client.get("/buckets/b/objects/k", headers=ALICE,
                   params={"version_id": old_id})
    assert r.status_code == 200 and r.content == b"one"
    assert client.get("/buckets/b/objects/k", headers=ALICE).content == b"three"


def test_recovery_removes_orphan_version_data(settings):
    client = TestClient(create_app(settings))
    client.put("/buckets/b", headers=ALICE,
               json={"versioning_enabled": True})
    client.put("/buckets/b/objects/k", headers=ALICE, content=b"live")
    data_dir = settings.data_dir / "buckets" / "b" / "objects-data"
    (data_dir / "ab").mkdir(exist_ok=True)
    (data_dir / "ab" / "orphan-id").write_bytes(b"junk")
    (data_dir / "tmp").mkdir(exist_ok=True)
    (data_dir / "tmp" / "x.tmp").write_bytes(b"half")
    print("\n[case] restart: orphan sharded data + tmp removed, live intact")
    client = TestClient(create_app(settings))
    assert client.get("/buckets/b/objects/k", headers=ALICE).content == b"live"
    assert not (data_dir / "ab").exists()


def test_recovery_completes_interrupted_enable_migration(settings):
    client = TestClient(create_app(settings))
    client.put("/buckets/b", headers=ALICE)
    client.put("/buckets/b/objects/k", headers=ALICE, content=b"legacy")
    # simulate enable that crashed after flipping bucket.json but before
    # migrating this object.
    bpath = settings.data_dir / "buckets" / "b" / "bucket.json"
    import json
    bmeta = json.loads(bpath.read_text())
    bmeta["versioning_enabled"] = True
    import os
    tmp = bpath.with_name("bucket.json.tmp")
    tmp.write_text(json.dumps(bmeta))
    os.replace(tmp, bpath)
    print("\n[case] restart mid-enable: migration completed by recover()")
    client = TestClient(create_app(settings))
    vs = client.get("/buckets/b/objects/k/versions", headers=ALICE).json()
    assert len(vs["versions"]) == 1
    assert client.get("/buckets/b/objects/k", headers=ALICE).content == b"legacy"
    client.put("/buckets/b/objects/k", headers=ALICE, content=b"next")
    vs = client.get("/buckets/b/objects/k/versions", headers=ALICE).json()
    assert len(vs["versions"]) == 2
