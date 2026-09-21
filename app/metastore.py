"""Crash-safe metadata/byte persistence primitives.

Atomicity: write to a unique temp file in the *same directory*, fsync it,
then os.replace() (atomic on POSIX) and fsync the parent directory. A crash
at any point therefore leaves either the old file or the complete new one —
never a torn metadata record or an unreferenced visible blob.

All blocking disk syscalls (write/fsync/replace/unlink) have async wrappers
that run in a worker thread: fsync may block for noticeable time on some
filesystems and must never freeze the event loop (this also keeps streaming
concurrent requests responsive under heavy IO).
"""
from __future__ import annotations

import asyncio
import json
import os
import tempfile
from pathlib import Path
from typing import Any

from app.errors import ErrorCode, error


def _fsync_dir(path: Path) -> None:
    fd = os.open(path, os.O_RDONLY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def atomic_write_bytes(path: Path, payload: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp_name = tempfile.mkstemp(prefix=f".{path.name}.", suffix=".tmp",
                                    dir=path.parent)
    tmp = Path(tmp_name)
    try:
        with os.fdopen(fd, "wb") as fh:
            fh.write(payload)
            fh.flush()
            os.fsync(fh.fileno())
        os.replace(tmp, path)
        _fsync_dir(path.parent)
    except BaseException:
        tmp.unlink(missing_ok=True)
        raise


async def async_write_bytes(path: Path, payload: bytes) -> None:
    await asyncio.to_thread(atomic_write_bytes, path, payload)


def atomic_write_json(path: Path, data: Any) -> None:
    atomic_write_bytes(
        path,
        (json.dumps(data, ensure_ascii=False, sort_keys=True) + "\n").encode("utf-8"),
    )


async def async_write_json(path: Path, data: Any) -> None:
    payload = (json.dumps(data, ensure_ascii=False, sort_keys=True) + "\n").encode()
    await asyncio.to_thread(atomic_write_bytes, path, payload)


def read_json(path: Path) -> Any:
    try:
        with open(path, "rb") as fh:
            return json.loads(fh.read().decode("utf-8"))
    except FileNotFoundError:
        raise error(ErrorCode.INTERNAL, f"metadata missing: {path.name}")
    except (json.JSONDecodeError, UnicodeDecodeError) as exc:
        raise error(ErrorCode.INTERNAL, f"corrupt metadata file {path.name}: {exc}")


def remove_file(path: Path) -> None:
    """Best-effort unlink + fsync so removal is durable too."""
    try:
        path.unlink(missing_ok=True)
        _fsync_dir(path.parent)
    except OSError:
        pass


async def async_remove_file(path: Path) -> None:
    await asyncio.to_thread(remove_file, path)


def remove_dir(path: Path) -> None:
    """Remove a leaf directory tree created by this store (uploads only)."""
    import shutil
    if path.exists():
        shutil.rmtree(path, ignore_errors=True)
        if path.parent.exists():
            _fsync_dir(path.parent)


async def async_remove_dir(path: Path) -> None:
    await asyncio.to_thread(remove_dir, path)
