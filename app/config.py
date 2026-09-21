"""Runtime configuration with configurable, bounded limits.

Values come from environment variables so deployments can tune without code
changes; every limit has a safe default and a hard upper bound.
"""
from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

# Safe defaults
_DEFAULT_MAX_OBJECT_SIZE = 5 * 1024 * 1024 * 1024   # 5 GiB
_DEFAULT_MAX_LIST_KEYS = 1000
_DEFAULT_STREAM_CHUNK = 1024 * 1024                 # 1 MiB
_DEFAULT_MAX_UPLOADS_PER_OBJECT = 100
_DEFAULT_MAX_INFLIGHT_WRITES = 32
_DEFAULT_MIN_PART_SIZE = 5 * 1024 * 1024     # S3-style: 5 MiB, except last part

# Hard upper bounds (unbounded memory/disk growth is never allowed)
_HARD_MAX_LIST_KEYS = 10_000
_HARD_STREAM_CHUNK = 64 * 1024 * 1024


def _env_int(name: str, default: int) -> int:
    raw = os.environ.get(name)
    if raw is None or raw.strip() == "":
        return default
    try:
        value = int(raw)
    except ValueError:
        return default
    return value if value > 0 else default


@dataclass(frozen=True)
class Settings:
    data_dir: Path
    max_object_size: int
    max_list_keys: int
    stream_chunk_size: int
    max_uploads_per_object: int
    max_inflight_writes: int
    min_part_size: int
    abort_orphan_uploads: bool


def load_settings(data_dir: str | None = None) -> Settings:
    root = Path(data_dir or os.environ.get("OSG_DATA_DIR", "./.osg-data")).resolve()

    max_list = min(
        _env_int("OSG_MAX_LIST_KEYS", _DEFAULT_MAX_LIST_KEYS),
        _HARD_MAX_LIST_KEYS,
    )
    chunk = min(
        _env_int("OSG_STREAM_CHUNK_SIZE", _DEFAULT_STREAM_CHUNK),
        _HARD_STREAM_CHUNK,
    )
    return Settings(
        data_dir=root,
        max_object_size=_env_int("OSG_MAX_OBJECT_SIZE", _DEFAULT_MAX_OBJECT_SIZE),
        max_list_keys=max_list,
        stream_chunk_size=max(chunk, 4096),
        max_uploads_per_object=_env_int(
            "OSG_MAX_UPLOADS_PER_OBJECT", _DEFAULT_MAX_UPLOADS_PER_OBJECT
        ),
        max_inflight_writes=_env_int(
            "OSG_MAX_INFLIGHT_WRITES", _DEFAULT_MAX_INFLIGHT_WRITES
        ),
        min_part_size=_env_int("OSG_MIN_PART_SIZE", _DEFAULT_MIN_PART_SIZE),
        abort_orphan_uploads=os.environ.get("OSG_ABORT_ORPHAN_UPLOADS", "1") not in (
            "0", "false", "False", ""
        ),
    )
