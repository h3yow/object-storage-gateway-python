"""Gateway configuration.

All limits are explicit so behaviour under pressure is a policy decision,
not an accident. Values can be overridden via OSG_* environment variables.
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path


def _int_env(name: str, default: int) -> int:
    raw = os.environ.get(name)
    return int(raw) if raw is not None else default


@dataclass
class Settings:
    # Root directory holding all metadata + object data.
    data_dir: Path = field(default_factory=lambda: Path(os.environ.get("OSG_DATA_DIR", "./osg-data")))
    # Hard cap on a single object's size (bytes). Larger writes are rejected.
    max_object_size: int = field(default_factory=lambda: _int_env("OSG_MAX_OBJECT_SIZE", 64 * 1024 * 1024))
    # Max keys returned by one list call; callers paginate with a marker.
    max_list_keys: int = field(default_factory=lambda: _int_env("OSG_MAX_LIST_KEYS", 1000))
    # Total bytes of object data the gateway will store. 0 = unlimited.
    quota_bytes: int = field(default_factory=lambda: _int_env("OSG_QUOTA_BYTES", 0))
    # Max concurrent in-flight multipart uploads. 0 = unlimited.
    max_active_uploads: int = field(default_factory=lambda: _int_env("OSG_MAX_ACTIVE_UPLOADS", 0))
    # Max bytes buffered in memory per read/list chunk.
    stream_chunk_size: int = field(default_factory=lambda: _int_env("OSG_STREAM_CHUNK_SIZE", 1024 * 1024))
    # Minimum part size for multipart uploads (except the last part).
    min_part_size: int = field(default_factory=lambda: _int_env("OSG_MIN_PART_SIZE", 5 * 1024 * 1024))

    def ensure_dirs(self) -> None:
        self.data_dir.mkdir(parents=True, exist_ok=True)
