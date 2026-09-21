"""Request identity and bucket-policy authorization.

Rules (deterministic and explainable):
  * Every request carries identity via the X-OSG-Principal header.
  * The bucket owner is implicitly allowed every action.
  * Statements are evaluated: an explicit ``deny`` always wins; otherwise an
    ``allow`` matching (principal, action) grants access; default is deny.
  * Action "*" in a statement matches every action.

The store calls authorize() *before* looking at object existence, so a denied
principal receives ACCESS_DENIED for both missing and present keys — existence
cannot be probed through error codes.
"""
from __future__ import annotations

from dataclasses import dataclass

from app.errors import ErrorCode, error
from app.models import BucketPolicy

PRINCIPAL_HEADER = "x-osg-principal"


@dataclass(frozen=True)
class Principal:
    name: str


def principal_from_headers(headers: dict[str, str]) -> Principal:
    name = headers.get(PRINCIPAL_HEADER)
    if not name:
        raise error(
            ErrorCode.UNAUTHORIZED,
            f"missing identity header {PRINCIPAL_HEADER!r}",
        )
    name = name.strip()
    if not name or len(name) > 128 or "\n" in name or "\r" in name:
        raise error(ErrorCode.UNAUTHORIZED, "invalid principal identity")
    return Principal(name)


def _action_matches(pattern: str, action: str) -> bool:
    return pattern == "*" or pattern == action


def evaluate(policy: BucketPolicy | None, principal_name: str,
             action: str, resource_owner: str | None) -> tuple[bool, str]:
    """Return (allowed, reason). Pure function; reason is logged in tests."""
    if resource_owner is not None and principal_name == resource_owner:
        return True, f"principal {principal_name!r} owns the resource"

    statements = list(policy.statements) if policy else []

    # 1. explicit deny first
    for st in statements:
        if (st.principal == principal_name and st.effect == "deny"
                and any(_action_matches(a, action) for a in st.actions)):
            return False, f"explicit deny statement for action {action!r}"
    # 2. then allow
    for st in statements:
        if (st.principal == principal_name and st.effect == "allow"
                and any(_action_matches(a, action) for a in st.actions)):
            return True, f"allow statement matches action {action!r}"
    # 3. default deny
    return False, f"no allow statement for principal {principal_name!r}, action {action!r}"


def authorize(policy: BucketPolicy | None, principal: Principal, action: str,
              resource_owner: str | None) -> None:
    allowed, _reason = evaluate(policy, principal.name, action, resource_owner)
    if not allowed:
        # Deliberately generic externally; details appear in server/test logs.
        raise error(ErrorCode.ACCESS_DENIED, "access denied")
