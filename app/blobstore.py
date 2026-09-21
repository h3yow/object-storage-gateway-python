"""Chunked streaming blob IO with hard size bounds.

Nothing here ever buffers a whole object: the network stream is consumed in
blocks of `chunk_size`, handed to a bounded in-memory queue (at most 2 blocks
queued) and spooled/fsync'ed by a dedicated worker *thread*. Blocking disk
syscalls therefore never stall the event loop, while memory stays bounded.

Beyond max_bytes the upload is rejected, the temp file removed, and no
reference to it is ever published in metadata.
"""
from __future__ import annotations

import asyncio
import hashlib
import os
import queue
import tempfile
from collections.abc import AsyncIterator
from pathlib import Path

from app.errors import ErrorCode, GatewayError, error

_EOF = object()


def _sink_worker(dest_dir: Path, max_bytes: int, chunk_size: int,
                 q: queue.Queue) -> tuple[Path, int, str]:
    """Blocking consumer running in a worker thread."""
    dest_dir.mkdir(parents=True, exist_ok=True)
    fd, tmp_name = tempfile.mkstemp(prefix=".incoming-", suffix=".part",
                                    dir=dest_dir)
    tmp = Path(tmp_name)
    hasher = hashlib.sha256()
    size = 0
    try:
        with os.fdopen(fd, "wb") as fh:
            while True:
                item = q.get()
                if item is _EOF:
                    break
                if isinstance(item, BaseException):
                    raise item
                size += len(item)
                if size > max_bytes:
                    raise error(
                        ErrorCode.ENTITY_TOO_LARGE,
                        f"object exceeds maximum size of {max_bytes} bytes "
                        f"(received >= {size})",
                    )
                hasher.update(item)
                view = memoryview(item)
                for off in range(0, len(view), chunk_size):
                    fh.write(view[off:off + chunk_size])
                fh.flush()
                os.fsync(fh.fileno())
        return tmp, size, hasher.hexdigest()
    except BaseException:
        tmp.unlink(missing_ok=True)
        raise


async def stream_sink(
    dest_dir: Path,
    chunks: AsyncIterator[bytes],
    max_bytes: int,
    chunk_size: int,
) -> tuple[Path, int, str]:
    """Spool *chunks* into a private temp file under dest_dir.

    Returns (temp_path, size, sha256). The caller owns the temp file: it must
    atomically publish or delete it. On size overflow or client failure the
    temp file is removed before propagating the error.
    """
    # Bounded queue: back-pressure is applied to the network reader, so even a
    # slow disk cannot cause unbounded buffering.
    q: queue.Queue = queue.Queue(maxsize=2)
    worker = asyncio.to_thread(_sink_worker, dest_dir, max_bytes,
                               chunk_size, q)
    worker_task = asyncio.ensure_future(worker)
    try:
        async for chunk in chunks:
            if not chunk:
                continue
            await asyncio.to_thread(q.put, chunk)
        await asyncio.to_thread(q.put, _EOF)
        return await worker_task
    except (GatewayError, OSError, RuntimeError, asyncio.CancelledError):
        # Unblock the worker and let it delete its temp file, then surface the
        # original failure. The worker's own cleanup outcome is irrelevant here.
        await asyncio.to_thread(q.put, _EOF)
        try:
            await worker_task
        except (GatewayError, OSError, RuntimeError, asyncio.CancelledError):
            pass  # secondary: original failure below is what the caller sees
        raise


def _concat_parts(srcs: list[Path], dest: Path, read_size: int) -> int:
    """Merge part files in order into a fresh fsync'ed file (runs in thread)."""
    fd, tmp_name = tempfile.mkstemp(prefix=f".{dest.name}.", suffix=".tmp",
                                    dir=dest.parent)
    tmp = Path(tmp_name)
    hasher = hashlib.sha256()
    total = 0
    try:
        with os.fdopen(fd, "wb") as out:
            for src in srcs:
                with open(src, "rb") as fh:
                    while True:
                        block = fh.read(read_size)
                        if not block:
                            break
                        total += len(block)
                        hasher.update(block)
                        out.write(block)
            out.flush()
            os.fsync(out.fileno())
        os.replace(tmp, dest)
        return total, hasher.hexdigest()
    except BaseException:
        tmp.unlink(missing_ok=True)
        raise


async def async_concat_parts(srcs: list[Path], dest: Path,
                             read_size: int) -> tuple[int, str]:
    return await asyncio.to_thread(_concat_parts, srcs, dest, read_size)


async def iter_file(path: Path, chunk_size: int,
                    start: int = 0, end: int | None = None) -> AsyncIterator[bytes]:
    """Yield at most chunk_size bytes per iteration (bounded memory).

    Each blocking read runs in a worker thread so a slow filesystem never
    blocks the event loop.
    """
    # File handle intentionally outlives the opening thread hop; it is closed
    # in the finally block below, so the context-manager lint does not apply.
    def _open():
        fh = open(path, "rb")  # noqa: SIM115
        if start:
            fh.seek(start)
        return fh

    fh = await asyncio.to_thread(_open)
    remaining = None if end is None else (end - start + 1)
    try:
        while True:
            limit = chunk_size if remaining is None else min(chunk_size, remaining)
            if limit <= 0:
                break
            block = await asyncio.to_thread(fh.read, limit)
            if not block:
                break
            if remaining is not None:
                remaining -= len(block)
            yield block
    finally:
        await asyncio.to_thread(fh.close)


def sha256_file(path: Path) -> str:
    hasher = hashlib.sha256()
    with open(path, "rb") as fh:
        for block in iter(lambda: fh.read(1024 * 1024), b""):
            hasher.update(block)
    return hasher.hexdigest()
