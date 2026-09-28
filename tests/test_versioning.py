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


def test_enabling_on_existing_object_gives_current_a_stable_id(client):
    """Enabling retention on a bucket that already holds objects must give
    the *current* content a stable, re-referenceable id immediately -- not
    only after the next overwrite."""
    client.put("/buckets/b", headers=ALICE)
    _put(client, "k", b"pre-existing")
    _put(client, "other", b"other-body")
    print("\n[case] enable retention: every existing current is migrated now")
    b = _enable(client, enabled=True)
    assert b["versioning_enabled"] and b["versioned_ever"]

    for key, body in (("k", b"pre-existing"), ("other", b"other-body")):
        vs = _versions(client, key)
        assert len(vs) == 1 and vs[0]["is_current"]
        vid = vs[0]["version_id"]
        assert vid  # a real, stable id, not empty
        assert vs[0]["size"] == len(body)
        r = client.get(f"/buckets/b/version/{vid}/{key}", headers=ALICE)
        assert r.status_code == 200 and r.content == body
        assert r.headers["X-ETag"] == vs[0]["etag"]
        # the ordinary current read advertises the very same id
        head = client.head(f"/buckets/b/objects/{key}", headers=ALICE)
        assert head.headers["X-Version-Id"] == vid

    print("[case] later overwrites continue from the migrated version 1")
    o2 = _put(client, "k", b"next")
    vs = _versions(client, "k")
    assert [v["version"] for v in vs] == [2, 1]
    assert vs[0]["version_id"] == o2["version_id"]
    # the migrated content is still the readable historical version 1
    v1 = vs[1]
    assert v1["version_id"] and client.get(
        f"/buckets/b/version/{v1['version_id']}/k", headers=ALICE).content == b"pre-existing"


def test_current_read_self_consistent_under_concurrent_overwrite(client, thread_client):
    """Reading the *current* object while writers overwrite it: the returned
    length/etag headers and the body must always describe one single
    version, in a versioned bucket (inode-pinned version bodies)."""
    _enable(client, enabled=True)
    _put(client, "hot", b"init")
    print("\n[case] current reads vs concurrent overwriters (versioned on)")
    stop = threading.Event()
    errors = []

    def writer(i):
        c = thread_client()
        j = 0
        while not stop.is_set():
            try:
                r = c.put("/buckets/b/objects/hot", headers=ALICE,
                          content=f"w{i}-{j:05d}-".encode() + b"c" * 512)
                assert r.status_code == 200, r.text
            except Exception as e:  # noqa: BLE001
                errors.append(e)
            j += 1

    def reader():
        c = thread_client()
        import hashlib
        import time
        deadline = time.time() + 2.0
        while time.time() < deadline:
            r = c.get("/buckets/b/objects/hot", headers=ALICE)
            if r.status_code != 200:
                errors.append(AssertionError(f"read {r.status_code}"))
                return
            body = r.content
            if int(r.headers["Content-Length"]) != len(body):
                errors.append(AssertionError("length/header mismatch"))
            if hashlib.sha256(body).hexdigest() != r.headers["X-ETag"]:
                errors.append(AssertionError("etag/body mismatch"))
            if not body.startswith(b"w") or b"c" * 512 not in body:
                errors.append(AssertionError("torn/mixed body"))

    threads = [threading.Thread(target=writer, args=(i,)) for i in range(4)]
    threads += [threading.Thread(target=reader) for _ in range(4)]
    for t in threads:
        t.start()
    import time
    time.sleep(2.5)
    stop.set()
    for t in threads:
        t.join()
    assert not errors, errors[:3]
    print("  every current read returned matching length+etag+whole body")


def test_current_read_self_consistent_legacy_bucket(client, thread_client):
    """Same guarantee for a bucket where retention was never enabled: current
    reads are snapshot-pinned against in-place legacy overwrites too."""
    client.put("/buckets/b", headers=ALICE)
    client.put("/buckets/b/objects/hot", headers=ALICE, content=b"init")
    print("\n[case] current reads vs concurrent overwriters (never versioned)")
    stop = threading.Event()
    errors = []

    def writer(i):
        c = thread_client()
        j = 0
        while not stop.is_set():
            try:
                r = c.put("/buckets/b/objects/hot", headers=ALICE,
                          content=f"w{i}-{j:05d}-".encode() + b"l" * 512)
                assert r.status_code == 200, r.text
            except Exception as e:  # noqa: BLE001
                errors.append(e)
            j += 1

    def reader():
        c = thread_client()
        import hashlib
        import time
        deadline = time.time() + 2.0
        while time.time() < deadline:
            r = c.get("/buckets/b/objects/hot", headers=ALICE)
            body = r.content
            if (int(r.headers["Content-Length"]) != len(body)
                    or hashlib.sha256(body).hexdigest() != r.headers["X-ETag"]
                    or not body.startswith(b"w") or b"l" * 512 not in body):
                errors.append(AssertionError("legacy header/body mismatch"))

    threads = [threading.Thread(target=writer, args=(i,)) for i in range(4)]
    threads += [threading.Thread(target=reader) for _ in range(4)]
    for t in threads:
        t.start()
    import time
    time.sleep(2.5)
    stop.set()
    for t in threads:
        t.join()
    assert not errors, errors[:3]
    # ordinary non-versioned behaviour is untouched: no version storage, empty id
    assert client.head("/buckets/b/objects/hot",
                       headers=ALICE).headers["X-Version-Id"] == ""
    data = client.app.state.settings.data_dir / "buckets" / "b" / "versions-data"
    assert not any(data.iterdir())
    print("  legacy reads self-consistent; no version storage created")


def test_version_listing_matches_reads_under_concurrent_overwrite(client, thread_client):
    """The version listed as current must be exactly what a by-id read and
    the current object read return, even while overwrites land in between
    the list and the reads (list is a single snapshot)."""
    _enable(client, enabled=True)
    _put(client, "hot", b"init")
    print("\n[case] list -> read current id -> read object, racing writers")
    stop = threading.Event()
    errors = []

    def writer(i):
        c = thread_client()
        j = 0
        while not stop.is_set():
            try:
                r = c.put("/buckets/b/objects/hot", headers=ALICE,
                          content=f"s{i}-{j:05d}".encode() + b"v" * 256)
                assert r.status_code == 200, r.text
            except Exception as e:  # noqa: BLE001
                errors.append(e)
            j += 1

    def reader():
        c = thread_client()
        import hashlib
        import time
        deadline = time.time() + 2.0
        while time.time() < deadline:
            r = c.get("/buckets/b/versions/hot", headers=ALICE)
            if r.status_code != 200:
                errors.append(AssertionError(f"list {r.status_code}"))
                return
            vs = r.json()["versions"]
            currents = [v for v in vs if v["is_current"]]
            if len(currents) != 1:
                errors.append(AssertionError("not exactly one current"))
                return
            cur = currents[0]
            # by-id read of the listed current id; the current id must never 404
            rv = c.get(f"/buckets/b/version/{cur['version_id']}/hot", headers=ALICE)
            if rv.status_code == 404:
                errors.append(AssertionError("listed current id not found"))
                continue
            body = rv.content
            if (rv.headers["X-ETag"] != cur["etag"]
                    or len(body) != cur["size"]
                    or hashlib.sha256(body).hexdigest() != cur["etag"]):
                errors.append(AssertionError("listed current vs read mismatch"))
            nums = [v["version"] for v in vs]
            if nums != sorted(nums, reverse=True):
                errors.append(AssertionError(f"bad version order {nums}"))
            # the ordinary current read is a separate request and may land on
            # a newer commit than the listing; it must still be self-consistent
            ro = c.get("/buckets/b/objects/hot", headers=ALICE)
            obody = ro.content
            if (int(ro.headers["Content-Length"]) != len(obody)
                    or hashlib.sha256(obody).hexdigest() != ro.headers["X-ETag"]):
                errors.append(AssertionError("current read header/body mismatch"))

    threads = [threading.Thread(target=writer, args=(i,)) for i in range(3)]
    threads += [threading.Thread(target=reader) for _ in range(3)]
    for t in threads:
        t.start()
    import time
    time.sleep(2.5)
    stop.set()
    for t in threads:
        t.join()
    assert not errors, errors[:3]
    print("  listed current always readable by id with matching header/body")


def test_pinned_current_read_racing_delete_is_all_or_nothing(client, thread_client):
    """A delete racing a current read must not leave the reader half-way
    through a removed body: the response is either 200 with the complete
    self-consistent content or 404 -- in both a versioned and a
    never-versioned bucket (versioned rename/unlink vs legacy in-place)."""
    import hashlib
    import time

    def _run(bucket: str, versioned: bool):
        if versioned:
            r = client.put(f"/buckets/{bucket}/versioning", headers=ALICE,
                           json={"enabled": True})
            assert r.status_code == 200, r.text
        errors = []

        def cycle():
            cd = thread_client()
            i = 0
            deadline = time.time() + 1.5
            while time.time() < deadline:
                r = cd.put(f"/buckets/{bucket}/objects/hot", headers=ALICE,
                           content=f"r{i:05d}-".encode() + b"d" * 200000)
                assert r.status_code == 200, r.text
                r = cd.delete(f"/buckets/{bucket}/objects/hot", headers=ALICE)
                assert r.status_code in (200, 404)
                i += 1

        def reader():
            cr = thread_client()
            deadline = time.time() + 1.5
            while time.time() < deadline:
                r = cr.get(f"/buckets/{bucket}/objects/hot", headers=ALICE)
                if r.status_code == 200:
                    body = r.content
                    if (int(r.headers["Content-Length"]) != len(body)
                            or hashlib.sha256(body).hexdigest() != r.headers["X-ETag"]):
                        errors.append("partial/garbled body")
                elif r.status_code != 404:
                    errors.append(f"status {r.status_code}")

        threads = [threading.Thread(target=cycle) for _ in range(2)]
        threads += [threading.Thread(target=reader) for _ in range(4)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        assert not errors, errors[:3]

    print("\n[case] current reads racing overwrite+delete (versioned)")
    client.put("/buckets/b", headers=ALICE)
    _run("b", True)
    print("  versioned: only complete 200 or 404 seen")
    print("[case] current reads racing overwrite+delete (never versioned)")
    client.put("/buckets/l", headers=ALICE)
    _run("l", False)
    print("  legacy: only complete 200 or 404 seen, no version storage")
    data = client.app.state.settings.data_dir / "buckets" / "l" / "versions-data"
    assert not any(data.iterdir())


def test_missing_version_ids_are_distinguishable(client):
    """Unknown / evicted ids -> 404 NoSuchVersion; a deleted key -> 404
    NoSuchKey. The distinction must hold whether retention is on, off, or
    was toggled after writes."""
    _enable(client, max_versions=2)
    print("\n[case] evicted and fabricated ids report NoSuchVersion")
    o1 = _put(client, "k", b"v1-body")
    _put(client, "k", b"v2-body")
    _put(client, "k", b"v3-body")  # max_versions=2 -> v1 evicted
    r = client.get(f"/buckets/b/version/{o1['version_id']}/k", headers=ALICE)
    assert r.status_code == 404 and r.json()["error"]["code"] == "NoSuchVersion"
    r = client.get(f"/buckets/b/version/{'99'*16}/k", headers=ALICE)
    assert r.status_code == 404 and r.json()["error"]["code"] == "NoSuchVersion"

    print("[case] same after retention is switched off and key overwritten")
    client.put("/buckets/b/versioning", headers=ALICE, json={"enabled": False})
    o3 = next(v for v in _versions(client, "k") if v["version"] == 3)
    _put(client, "k", b"v4-body")  # drops v3 (replaced current), keeps frozen v2
    r = client.get(f"/buckets/b/version/{o3['version_id']}/k", headers=ALICE)
    assert r.status_code == 404 and r.json()["error"]["code"] == "NoSuchVersion"

    print("[case] deleting the key turns all its version lookups into NoSuchKey")
    r = client.delete("/buckets/b/objects/k", headers=ALICE)
    assert r.status_code == 200
    r = client.get("/buckets/b/versions/k", headers=ALICE)
    assert r.status_code == 404 and r.json()["error"]["code"] == "NoSuchKey"
    r = client.get(f"/buckets/b/version/{'ab'*16}/k", headers=ALICE)
    assert r.status_code == 404 and r.json()["error"]["code"] == "NoSuchKey"


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
    o2 = _put(client, "k", b"v2")
    print("\n[case] disable versioning, overwrite (frozen history), then delete")
    client.put("/buckets/b/versioning", headers=ALICE, json={"enabled": False})
    o3 = _put(client, "k", b"v3-current")  # still version-stored, stable id
    svc = client.app.state.service
    # disabled overwrite drops the replaced current (v2) but keeps earlier
    # frozen history (v1); the new current occupies exactly one body.
    before = svc.store.usage_bytes()
    assert before == len(b"v3-current") + len(b"v1")
    print("  usage after disabled overwrite:", before,
          "(current v3 + frozen v1; replaced v2 released)")

    print("[case] current is still listed with a stable id and is readable")
    vs = _versions(client, "k")
    assert [(v["version"], v["is_current"]) for v in vs] == [(3, True), (1, False)]
    cur = vs[0]
    assert cur["version_id"] == o3["version_id"] and cur["version_id"]
    r = client.get(f"/buckets/b/version/{cur['version_id']}/k", headers=ALICE)
    assert r.status_code == 200 and r.content == b"v3-current"
    assert client.get("/buckets/b/objects/k", headers=ALICE).content == b"v3-current"
    # older frozen history survives and is readable
    v1 = next(v for v in vs if v["version"] == 1)
    assert client.get(f"/buckets/b/version/{v1['version_id']}/k",
                      headers=ALICE).content == b"v1"
    print("[case] replaced current id is a distinguishable 'not found'")
    r = client.get(f"/buckets/b/version/{o2['version_id']}/k", headers=ALICE)
    assert r.status_code == 404 and r.json()["error"]["code"] == "NoSuchVersion"
    r = client.get(f"/buckets/b/version/{'ab'*16}/k", headers=ALICE)
    assert r.status_code == 404 and r.json()["error"]["code"] == "NoSuchVersion"

    print("[case] re-enabling resumes history preservation")
    client.put("/buckets/b/versioning", headers=ALICE, json={"enabled": True})
    _put(client, "k", b"v4")
    vs = _versions(client, "k")
    assert [(v["version"], v["is_current"]) for v in vs] == [(4, True), (3, False), (1, False)]

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
