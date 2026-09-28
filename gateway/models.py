"""Metadata models. Everything here is JSON-serialisable so the metadata
store can persist it atomically and recover it after a restart."""

from __future__ import annotations

import time
import uuid
from dataclasses import asdict, dataclass, field


def _now() -> float:
    return time.time()


def new_id() -> str:
    return uuid.uuid4().hex


@dataclass
class BucketMeta:
    name: str
    owner: str
    created_at: float = field(default_factory=_now)
    # Bucket policy: {action: [identity, ...]}; "*" matches any identity.
    # Actions: read, write, list, delete.
    policy: dict = field(default_factory=dict)
    # When True, overwrites/merges preserve the previous content as a
    # read-only historical version instead of destroying it.
    versioning_enabled: bool = False
    # Sticky: set True the first time retention is enabled and never reset.
    # While versioning_enabled can be toggled off, a bucket that once had
    # retention keeps every current committed through version storage, so the
    # current version always has a stable, re-referenceable version_id even
    # after retention is disabled (only new-history preservation stops).
    versioned_ever: bool = False
    # Retention limits for historical versions, per key. 0 = unlimited.
    # Oldest non-current versions are evicted first; the current version is
    # never evicted. Limits apply only while versioning_enabled is True.
    max_versions: int = 0        # keep at most this many versions per key
    max_version_bytes: int = 0   # cap total bytes of kept versions per key

    def to_dict(self) -> dict:
        return asdict(self)

    @classmethod
    def from_dict(cls, d: dict) -> "BucketMeta":
        return cls(name=d["name"], owner=d["owner"],
                   created_at=d["created_at"], policy=d.get("policy", {}),
                   versioning_enabled=d.get("versioning_enabled", False),
                   versioned_ever=d.get("versioned_ever", False),
                   max_versions=d.get("max_versions", 0),
                   max_version_bytes=d.get("max_version_bytes", 0))


@dataclass
class ObjectMeta:
    key: str
    size: int
    etag: str
    version: int = 1            # bumped on every committed overwrite
    created_at: float = field(default_factory=_now)
    updated_at: float = field(default_factory=_now)
    # Stable identifier of the current content. Empty for legacy objects
    # written before versioning existed (their data lives at the legacy path).
    version_id: str = ""

    def to_dict(self) -> dict:
        return asdict(self)

    @classmethod
    def from_dict(cls, d: dict) -> "ObjectMeta":
        return cls(key=d["key"], size=d["size"], etag=d["etag"],
                   version=d.get("version", 1), created_at=d["created_at"],
                   updated_at=d["updated_at"], version_id=d.get("version_id", ""))


@dataclass
class VersionMeta:
    """One immutable historical version of a key."""
    key: str
    version: int                # 1-based sequence number within the key
    version_id: str             # stable, re-referenceable identifier
    size: int
    etag: str
    created_at: float           # when this content became current

    def to_dict(self) -> dict:
        return asdict(self)

    @classmethod
    def from_dict(cls, d: dict) -> "VersionMeta":
        return cls(key=d["key"], version=d["version"], version_id=d["version_id"],
                   size=d["size"], etag=d["etag"], created_at=d["created_at"])


@dataclass
class PartMeta:
    number: int
    size: int
    etag: str

    def to_dict(self) -> dict:
        return asdict(self)

    @classmethod
    def from_dict(cls, d: dict) -> "PartMeta":
        return cls(number=d["number"], size=d["size"], etag=d["etag"])


@dataclass
class UploadMeta:
    upload_id: str
    key: str
    initiator: str
    created_at: float = field(default_factory=_now)
    parts: dict = field(default_factory=dict)  # str(number) -> PartMeta dict

    def to_dict(self) -> dict:
        return {"upload_id": self.upload_id, "key": self.key, "initiator": self.initiator,
                "created_at": self.created_at,
                "parts": {k: (v.to_dict() if isinstance(v, PartMeta) else v) for k, v in self.parts.items()}}

    @classmethod
    def from_dict(cls, d: dict) -> "UploadMeta":
        return cls(upload_id=d["upload_id"], key=d["key"], initiator=d["initiator"],
                   created_at=d["created_at"],
                   parts={k: PartMeta.from_dict(v) for k, v in d.get("parts", {}).items()})
