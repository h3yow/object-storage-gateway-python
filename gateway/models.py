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
    # Per-bucket history retention. When enabled, overwrites (plain PUT or
    # multipart complete) keep the previous content as a readable history
    # version instead of destroying it. The two caps apply to *non-current*
    # versions only; the current version is never evicted. 0 = unlimited.
    versioning_enabled: bool = False
    max_versions: int = 0
    max_version_bytes: int = 0

    def to_dict(self) -> dict:
        return asdict(self)

    @classmethod
    def from_dict(cls, d: dict) -> "BucketMeta":
        return cls(name=d["name"], owner=d["owner"], created_at=d["created_at"],
                   policy=d.get("policy", {}),
                   versioning_enabled=d.get("versioning_enabled", False),
                   max_versions=d.get("max_versions", 0),
                   max_version_bytes=d.get("max_version_bytes", 0))


@dataclass
class VersionRecord:
    """One immutable historical version of an object in a versioned bucket."""
    version_id: str
    version: int                # monotonic sequence number, bumped per commit
    size: int
    etag: str
    created_at: float = field(default_factory=_now)

    def to_dict(self) -> dict:
        return asdict(self)

    @classmethod
    def from_dict(cls, d: dict) -> "VersionRecord":
        return cls(version_id=d["version_id"], version=d["version"],
                   size=d["size"], etag=d["etag"], created_at=d["created_at"])


@dataclass
class ObjectMeta:
    key: str
    size: int
    etag: str
    version: int = 1            # bumped on every committed overwrite
    # Stable identifier of the current version ("" in legacy/unversioned
    # layout). In a versioned bucket every committed version has an id and
    # old ids can be used to read or restore that exact content.
    version_id: str = ""
    created_at: float = field(default_factory=_now)
    updated_at: float = field(default_factory=_now)

    def to_dict(self) -> dict:
        return asdict(self)

    @classmethod
    def from_dict(cls, d: dict) -> "ObjectMeta":
        return cls(key=d["key"], size=d["size"], etag=d["etag"],
                   version=d.get("version", 1), version_id=d.get("version_id", ""),
                   created_at=d["created_at"], updated_at=d["updated_at"])


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
