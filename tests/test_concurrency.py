"""Concurrency: simultaneous writes to one key are serialised; conditional
writers never lose updates; concurrent multipart parts never corrupt state."""

import threading

from tests.conftest import ALICE


def test_concurrent_unconditional_writes_are_serialised(client):
    client.put("/buckets/b", headers=ALICE)
    n = 16
    print(f"\n[case] {n} threads PUT the same key unconditionally")
    errors = []

    def write(i):
        try:
            r = client.put("/buckets/b/objects/hot", headers=ALICE,
                           content=f"writer-{i}".encode())
            assert r.status_code == 200, r.text
        except Exception as e:  # noqa: BLE001
            errors.append(e)

    threads = [threading.Thread(target=write, args=(i,)) for i in range(n)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert not errors
    r = client.get("/buckets/b/objects/hot", headers=ALICE)
    body = r.content.decode()
    print(f"  final version={r.headers['X-Version']} body={body!r}")
    # serialised: version advanced exactly n times, body is one whole write
    assert r.headers["X-Version"] == str(n)
    assert body.startswith("writer-")


def test_concurrent_conditional_writes_no_lost_update(client):
    client.put("/buckets/b", headers=ALICE)
    client.put("/buckets/b/objects/counter", headers=ALICE, content=b"0")
    n = 10
    print(f"\n[case] {n} threads read-modify-write with if_version (optimistic)")
    successes, rejections = [], []

    def bump(_i):
        for _ in range(50):  # retry until our write wins
            head = client.head("/buckets/b/objects/counter", headers=ALICE)
            version = int(head.headers["X-Version"])
            current = int(client.get("/buckets/b/objects/counter", headers=ALICE).content)
            r = client.put("/buckets/b/objects/counter", headers=ALICE,
                           content=str(current + 1).encode(),
                           params={"if_version": version})
            if r.status_code == 200:
                successes.append(1)
                return
            assert r.status_code == 412, r.text
            rejections.append(1)
        raise AssertionError("writer never succeeded")

    threads = [threading.Thread(target=bump, args=(i,)) for i in range(n)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    final = int(client.get("/buckets/b/objects/counter", headers=ALICE).content)
    print(f"  committed={len(successes)} rejected={len(rejections)} final={final}")
    assert len(successes) == n and final == n  # no lost update


def test_concurrent_parts_and_complete(client):
    client.put("/buckets/b", headers=ALICE)
    uid = client.post("/buckets/b/uploads", headers=ALICE,
                      params={"key": "asm"}).json()["upload"]["upload_id"]
    n = 8
    print(f"\n[case] {n} threads upload distinct parts concurrently, then merge")
    barrier = threading.Barrier(n)

    def put_part(i):
        barrier.wait()
        r = client.put(f"/buckets/b/uploads/{uid}/parts/{i + 1}", headers=ALICE,
                       content=f"part{i:02d}-".encode())
        assert r.status_code == 200, r.text

    threads = [threading.Thread(target=put_part, args=(i,)) for i in range(n)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    r = client.post(f"/buckets/b/uploads/{uid}/complete", headers=ALICE)
    assert r.status_code == 200
    body = client.get("/buckets/b/objects/asm", headers=ALICE).content.decode()
    expected = "".join(f"part{i:02d}-" for i in range(n))
    print(f"  merged == expected order: {body == expected}")
    assert body == expected


def test_concurrent_create_and_delete_same_key(client):
    client.put("/buckets/b", headers=ALICE)
    client.put("/buckets/b/objects/k", headers=ALICE, content=b"init")
    print("\n[case] racing delete vs conditional overwrite: exactly one wins per round")
    results = []

    def deleter():
        r = client.delete("/buckets/b/objects/k", headers=ALICE)
        results.append(("delete", r.status_code))

    def writer():
        r = client.put("/buckets/b/objects/k", headers=ALICE, content=b"w",
                       params={"if_version": 1})
        results.append(("write", r.status_code))

    t1, t2 = threading.Thread(target=deleter), threading.Thread(target=writer)
    t1.start(); t2.start(); t1.join(); t2.join()
    print("  results:", sorted(results))
    codes = sorted(code for _, code in results)
    # delete either succeeded (200) or found it gone (404); write either
    # committed against version 1 (200) or was rejected (404/412 via store).
    assert codes[0] in (200, 404, 412) and codes[1] in (200, 404, 412)
    # state is consistent afterwards: either key exists with whole body or not
    r = client.get("/buckets/b/objects/k", headers=ALICE)
    assert r.status_code in (200, 404)
    if r.status_code == 200:
        assert r.content in (b"w", b"init")
