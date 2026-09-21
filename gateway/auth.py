"""Identity and bucket-policy based authorisation.

Identity arrives via the ``X-Identity`` header (a local stand-in for real
credentials; no external IdP is involved). Each bucket carries a policy
mapping action -> list of allowed identities, where "*" matches everyone.
The bucket owner is always allowed every action.

Key property: authorisation failure and missing resource must be
indistinguishable to the caller, so the service layer checks existence
first and then raises AccessDenied for *both* "no such bucket" and
"exists but you may not see it" when the caller lacks even read rights.
"""

from __future__ import annotations

from .errors import AccessDenied, InvalidRequest
from .models import BucketMeta

ACTIONS = ("read", "write", "list", "delete")

HEADER = "x-identity"


def parse_identity(headers) -> str:
    identity = headers.get(HEADER, "").strip()
    if not identity:
        raise AccessDenied("missing identity header: X-Identity")
    return identity


def is_allowed(bucket: BucketMeta, identity: str, action: str) -> bool:
    if action not in ACTIONS:
        raise InvalidRequest(f"unknown action: {action}")
    if identity == bucket.owner:
        return True
    allowed = bucket.policy.get(action, [])
    return "*" in allowed or identity in allowed


def require(bucket: BucketMeta, identity: str, action: str) -> None:
    if not is_allowed(bucket, identity, action):
        raise AccessDenied(f"identity '{identity}' is not allowed to {action} bucket '{bucket.name}'")
