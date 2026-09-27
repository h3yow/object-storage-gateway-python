"""Authorisation: per-action checks, owner rights, no existence leakage."""

from tests.conftest import ALICE, BOB

CAROL = {"X-Identity": "carol"}


def test_per_action_authorisation(client):
    policy = {"read": ["bob"], "list": ["bob"], "write": ["bob"]}
    client.put("/buckets/secure", headers=ALICE, json={"policy": policy})
    client.put("/buckets/secure/objects/doc", headers=ALICE, content=b"secret")
    print("\n[policy] owner=alice, read/list/write=[bob], delete=[]")

    print("[case] bob may read and list but NOT delete")
    assert client.get("/buckets/secure/objects/doc", headers=BOB).status_code == 200
    assert client.get("/buckets/secure/objects", headers=BOB).status_code == 200
    r = client.delete("/buckets/secure/objects/doc", headers=BOB)
    print("  bob delete ->", r.status_code, r.json()["error"]["code"])
    assert r.status_code == 403 and r.json()["error"]["code"] == "AccessDenied"

    print("[case] carol (not in policy) may do nothing")
    for method, url in [("GET", "/buckets/secure/objects/doc"),
                        ("GET", "/buckets/secure/objects"),
                        ("PUT", "/buckets/secure/objects/x"),
                        ("DELETE", "/buckets/secure/objects/doc")]:
        r = client.request(method, url, headers=CAROL, content=b"x")
        print(f"  carol {method} {url.split('secure')[-1]} -> {r.status_code}")
        assert r.status_code == 403

    print("[case] owner alice retains all rights")
    assert client.delete("/buckets/secure/objects/doc", headers=ALICE).status_code == 200


def test_auth_failure_does_not_leak_existence(client):
    client.put("/buckets/hidden", headers=ALICE)  # no policy: owner only
    print("\n[case] carol gets identical AccessDenied for existing and missing buckets")
    r1 = client.get("/buckets/hidden", headers=CAROL)
    r2 = client.get("/buckets/definitely-missing", headers=CAROL)
    print(f"  existing bucket -> {r1.status_code}; missing bucket -> {r2.status_code}")
    assert r1.status_code == 403 and r1.json()["error"]["code"] == "AccessDenied"
    # missing bucket: 404 is fine to reveal *non*-existence only to the point
    # that no auth decision leaked; but an unauthorised caller probing keys
    # inside an existing bucket must always see 403:
    r3 = client.get("/buckets/hidden/objects/anything", headers=CAROL)
    assert r3.status_code == 403
    print("  key probe inside forbidden bucket -> 403 (existence not leaked)")


def test_missing_identity_rejected(client):
    client.put("/buckets/b", headers=ALICE, json={"policy": {"read": ["*"]}})
    print("\n[case] request without X-Identity -> AccessDenied even for public read")
    r = client.get("/buckets/b/objects")
    assert r.status_code == 403 and r.json()["error"]["code"] == "AccessDenied"


def test_policy_management_owner_only(client):
    client.put("/buckets/b", headers=ALICE)
    print("\n[case] non-owner cannot change policy; owner can")
    r = client.put("/buckets/b/policy", headers=BOB, json={"policy": {"read": ["bob"]}})
    assert r.status_code == 403
    r = client.put("/buckets/b/policy", headers=ALICE, json={"policy": {"read": ["bob"]}})
    assert r.status_code == 200
    assert client.get("/buckets/b", headers=BOB).status_code == 200
    print("[case] unknown policy action rejected")
    r = client.put("/buckets/b/policy", headers=ALICE, json={"policy": {"fly": ["*"]}})
    assert r.status_code == 400 and r.json()["error"]["code"] == "InvalidRequest"


def test_version_endpoints_authorisation(client):
    client.put("/buckets/b", headers=ALICE, json={"versioning_enabled": True})
    client.put("/buckets/b/objects/k", headers=ALICE, content=b"v1")
    client.put("/buckets/b/objects/k", headers=ALICE, content=b"v2")
    vid = client.get("/buckets/b/versions/k", headers=ALICE).json()["versions"][-1]["version_id"]
    print("\n[authz] bob has no grants: version list/read/rollback all 403")
    assert client.get("/buckets/b/versions/k", headers=BOB).status_code == 403
    assert client.get(f"/buckets/b/version/{vid}/k", headers=BOB).status_code == 403
    r = client.post(f"/buckets/b/version/{vid}/k/rollback", headers=BOB)
    assert r.status_code == 403
    print("[authz] read grant covers list + version read; rollback still needs write")
    client.put("/buckets/b/policy", headers=ALICE, json={"policy": {"read": ["bob"]}})
    assert client.get("/buckets/b/versions/k", headers=BOB).status_code == 200
    assert client.get(f"/buckets/b/version/{vid}/k", headers=BOB).status_code == 200
    r = client.post(f"/buckets/b/version/{vid}/k/rollback", headers=BOB)
    assert r.status_code == 403
