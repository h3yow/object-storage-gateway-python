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

    def to_dict(self) -> dict:
        return asdict(self)

    @classmethod
    def from_dict(cls, d: dict) -> "BucketMeta":
        return cls(name=d["name"], owner=d["owner"], created_at=d["created_at"], policy=d.get("policy", {}))


@dataclass
class ObjectMeta:
    key: str
    size: int
    etag: str
    version: int = 1            # bumped on every committed overwrite
    created_at: float = field(default_factory=_now)
    updated_at: float = field(default_factory=_now)

    def to_dict(self) -> dict:
        return asdict(self)

    @classmethod
    def from_dict(cls, d: dict) -> "ObjectMeta":
        return cls(key=d["key"], size=d["size"], etag=d["etag"],
                   version=d.get("version", 1), created_at=d["created_at"], updated_at=d["updated_at"])


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
