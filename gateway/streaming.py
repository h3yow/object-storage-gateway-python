"""Bridge FastAPI's async request stream into the store's sync chunk
iterator without buffering the whole body in memory.

The store runs in a worker thread (anyio.to_thread) and pulls chunks from
a sync iterator; each pull schedules one __anext__ on the event loop and
blocks until it resolves. Backpressure is natural: the loop only reads the
next chunk when the store is ready to write it.
"""

from __future__ import annotations

import asyncio
from typing import AsyncGenerator, Iterator


def sync_chunks(agen: AsyncGenerator[bytes, None], loop: asyncio.AbstractEventLoop) -> Iterator[bytes]:
    async def _next():
        try:
            return await agen.__anext__()
        except StopAsyncIteration:
            return None

    try:
        while True:
            chunk = asyncio.run_coroutine_threadsafe(_next(), loop).result()
            if chunk is None:
                return
            yield chunk
    finally:
        if not loop.is_closed():
            try:
                asyncio.run_coroutine_threadsafe(agen.aclose(), loop).result()
            except RuntimeError:
                pass  # loop closed between check and schedule; nothing to clean
