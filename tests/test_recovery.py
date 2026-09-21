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
