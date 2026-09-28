"""Crash/restart recovery: metadata-data consistency, orphan cleanup,
resumable uploads surviving a restart."""

from fastapi.testclient import TestClient

from gateway.config import Settings
from main import create_app
from tests.conftest import ALICE


def _reopen(settings):
    app = create_app(settings)
    return TestClient(app), app.state.recovery_report


def test_orphan_data_and_tmp_removed_on_restart(settings):
    client = TestClient(create_app(settings))
    client.put("/buckets/b", headers=ALICE)
    client.put("/buckets/b/objects/k", headers=ALICE, content=b"live")
    data_dir = settings.data_dir / "buckets" / "b" / "objects-data"
    (data_dir / "orphan-hash").write_bytes(b"junk")       # data without metadata
    (data_dir / "half-written.tmp").write_bytes(b"junk")  # interrupted write
    print("\n[case] restart with orphan data file + stale tmp file")
    client, report = _reopen(settings)
    print("  recovery report:", report)
    assert report["orphan_data_removed"] == 1 and report["tmp_removed"] == 1
    assert client.get("/buckets/b/objects/k", headers=ALICE).content == b"live"
    print("  committed object intact after restart")


def test_interrupted_write_never_visible(settings):
    client = TestClient(create_app(settings))
    client.put("/buckets/b", headers=ALICE)
    client.put("/buckets/b/objects/k", headers=ALICE, content=b"original")

    def exploding_chunks():
        yield b"partial-"
        raise ConnectionResetError("client went away mid-body")

    print("\n[case] write interrupted mid-stream")
    try:
        client.app.state.service.put_object("alice", "b", "k", exploding_chunks())
    except ConnectionResetError:
        print("  writer raised mid-stream (simulated disconnect)")
    r = client.get("/buckets/b/objects/k", headers=ALICE)
    print(f"  object after interruption: {r.content!r} (must be the old whole value)")
    assert r.content == b"original"
    # no tmp residue
    leftovers = list((settings.data_dir / "buckets" / "b" / "objects-data").glob("*.tmp"))
    assert leftovers == []


def test_upload_resumable_after_restart(settings):
    client = TestClient(create_app(settings))
    client.put("/buckets/b", headers=ALICE)
    uid = client.post("/buckets/b/uploads", headers=ALICE,
                      params={"key": "big"}).json()["upload"]["upload_id"]
    client.put(f"/buckets/b/uploads/{uid}/parts/1", headers=ALICE, content=b"AAA")
    print(f"\n[case] restart with in-flight upload {uid[:8]} (1 part stored)")
    client, report = _reopen(settings)
    uploads = client.get("/buckets/b/uploads", headers=ALICE).json()["uploads"]
    print(f"  uploads after restart: {[u['upload_id'][:8] for u in uploads]}")
    assert [u["upload_id"] for u in uploads] == [uid]
    client.put(f"/buckets/b/uploads/{uid}/parts/2", headers=ALICE, content=b"BBB")
    assert client.post(f"/buckets/b/uploads/{uid}/complete", headers=ALICE).status_code == 200
    assert client.get("/buckets/b/objects/big", headers=ALICE).content == b"AAABBB"
    print("  resumed upload merged correctly")


def test_orphan_upload_data_removed_on_restart(settings):
    client = TestClient(create_app(settings))
    client.put("/buckets/b", headers=ALICE)
    uid = client.post("/buckets/b/uploads", headers=ALICE,
                      params={"key": "x"}).json()["upload"]["upload_id"]
    client.put(f"/buckets/b/uploads/{uid}/parts/1", headers=ALICE, content=b"P")
    # simulate a crash between removing upload metadata and its data dir
    (settings.data_dir / "buckets" / "b" / "uploads" / f"{uid}.json").unlink()
    print(f"\n[case] restart with part data whose upload metadata is gone ({uid[:8]})")
    client, report = _reopen(settings)
    print("  recovery report:", report)
    assert report["orphan_parts_removed"] == 1
    assert client.get("/buckets/b/uploads", headers=ALICE).json()["uploads"] == []


def test_interrupted_enable_migration_recovers_cleanly(settings):
    import hashlib
    import json
    import uuid
    client = TestClient(create_app(settings))
    client.put("/buckets/b", headers=ALICE)
    client.put("/buckets/b/objects/k", headers=ALICE, content=b"hello-body")
    bdir = settings.data_dir / "buckets" / "b"
    kh = hashlib.sha256(b"k").hexdigest()
    vid = uuid.uuid4().hex
    # simulate a crash mid-enable-migration: data copied + VersionMeta written,
    # ObjectMeta not yet repointed, bucket flag not committed, legacy body kept
    (bdir / "versions-data" / kh).mkdir(parents=True, exist_ok=True)
    (bdir / "versions" / kh).mkdir(parents=True, exist_ok=True)
    (bdir / "versions-data" / kh / vid).write_bytes(b"hello-body")
    (bdir / "versions" / kh / f"{vid}.json").write_text(json.dumps(
        {"key": "k", "version": 1, "version_id": vid, "size": 10,
         "etag": hashlib.sha256(b"hello-body").hexdigest(), "created_at": 1.0}))
    print("\n[case] restart after interrupted enable-migration is safe")
    client, report = _reopen(settings)
    print("  recovery report:", report)
    assert client.get("/buckets/b/objects/k", headers=ALICE).content == b"hello-body"
    # enabling now must reuse the frozen copy (idempotent), not duplicate it
    client.put("/buckets/b/versioning", headers=ALICE, json={"enabled": True})
    versions = client.get("/buckets/b/versions/k", headers=ALICE).json()["versions"]
    assert [(v["version"], v["is_current"]) for v in versions] == [(1, True)]
    assert versions[0]["version_id"] == vid
    assert client.get(f"/buckets/b/version/{vid}/k",
                      headers=ALICE).content == b"hello-body"
    metas = {p.stem for p in (bdir / "versions").rglob("*.json")}
    datas = {p.name for p in (bdir / "versions-data").rglob("*") if p.is_file()}
    assert metas == datas and metas == {vid}
    print("  migration retried idempotently: one version, one body")


def test_version_storage_orphans_recovered(settings):
    import hashlib
    import json
    client = TestClient(create_app(settings))
    client.put("/buckets/b", headers=ALICE)
    client.put("/buckets/b/versioning", headers=ALICE, json={"enabled": True})
    client.put("/buckets/b/objects/k", headers=ALICE, content=b"v1")
    client.put("/buckets/b/objects/k", headers=ALICE, content=b"v2")
    client.put("/buckets/b/objects/g", headers=ALICE, content=b"g1")
    bdir = settings.data_dir / "buckets" / "b"
    kh = hashlib.sha256(b"k").hexdigest()
    gh = hashlib.sha256(b"g").hexdigest()
    print("\n[case] restart with: dangling version data, a data-less VersionMeta, "
          "and a deleted key whose version files survived a crash mid-delete")
    # 1) data file without any VersionMeta / key
    (bdir / "versions-data" / "ghost-key").mkdir(parents=True)
    (bdir / "versions-data" / "ghost-key" / "deadbeef").write_bytes(b"junk")
    # 2) VersionMeta without data on a live key -> meta must be removed
    (bdir / "versions" / kh / "00ff00ff.json").write_text(json.dumps(
        {"key": "k", "version": 99, "version_id": "00ff00ff",
         "size": 4, "etag": "x", "created_at": 1.0}))
    # 3) simulate a crash during delete of g: current meta gone, versions left
    (bdir / "objects" / f"{gh}.json").unlink()
    client, report = _reopen(settings)
    print("  recovery report:", report)
    assert report["orphan_data_removed"] >= 3  # ghost data + meta-less + g's files
    # live object intact and its versions still readable
    assert client.get("/buckets/b/objects/k", headers=ALICE).content == b"v2"
    versions = client.get("/buckets/b/versions/k", headers=ALICE).json()["versions"]
    assert [v["version"] for v in versions] == [2, 1]
    # every remaining data file has a meta and vice versa; no half-cleaned dirs
    metas = {p.stem for p in (bdir / "versions").rglob("*.json")}
    datas = {p.name for p in (bdir / "versions-data").rglob("*") if p.is_file()}
    print("  meta/data files agree:", metas == datas, "count:", len(metas))
    assert metas == datas
    assert not (bdir / "versions" / gh).exists()
    assert not (bdir / "versions-data" / gh).exists()
