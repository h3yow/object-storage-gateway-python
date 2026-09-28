"""Bounded-memory behaviour: reads/writes are chunked by stream_chunk_size,
listing is capped by max_list_keys."""

from fastapi.testclient import TestClient

from gateway.config import Settings
from main import create_app
from tests.conftest import ALICE


def test_read_write_streamed_in_bounded_chunks(tmp_path):
    settings = Settings(data_dir=tmp_path / "d", stream_chunk_size=4, min_part_size=1)
    client = TestClient(create_app(settings))
    client.put("/buckets/b", headers=ALICE)
    payload = b"0123456789ABCDEF" * 100  # 1600 bytes, chunk size 4
    print(f"\n[stream] write {len(payload)} bytes with chunk_size={settings.stream_chunk_size}")
    assert client.put("/buckets/b/objects/s", headers=ALICE, content=payload).status_code == 200
    r = client.get("/buckets/b/objects/s", headers=ALICE)
    assert r.content == payload
    print("  round-trip intact; server-side read path yields <= chunk_size buffers")
    # verify the store read path really chunks
    store = client.app.state.service.store
    _, stream = store.read_object_stream("b", "s")
    chunks = list(stream)
    print(f"  chunks yielded: {len(chunks)}, max chunk: {max(len(c) for c in chunks)}")
    assert max(len(c) for c in chunks) <= settings.stream_chunk_size
    assert b"".join(chunks) == payload


def test_list_limit_capped_by_server_max(tmp_path):
    settings = Settings(data_dir=tmp_path / "d", max_list_keys=3, min_part_size=1)
    client = TestClient(create_app(settings))
    client.put("/buckets/b", headers=ALICE)
    for i in range(5):
        client.put(f"/buckets/b/objects/k{i}", headers=ALICE, content=b"x")
    print("\n[stream] client asks limit=100 but server max_list_keys=3")
    r = client.get("/buckets/b/objects", headers=ALICE, params={"limit": 100})
    page = r.json()
    print(f"  returned {len(page['objects'])} keys, truncated={page['truncated']}")
    assert len(page["objects"]) == 3 and page["truncated"]
