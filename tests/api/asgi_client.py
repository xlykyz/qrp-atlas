"""Small ASGI test client for API route tests.

FastAPI/Starlette's synchronous TestClient hangs in the current
Python 3.14 + anyio 4.13 environment. httpx.ASGITransport works for async
endpoints, but sync endpoints still go through anyio's threadpool path, which
hits the same hang. The route tests only need deterministic in-process
requests, so this helper runs sync endpoints inline for the duration of each
request.
"""

from __future__ import annotations

import asyncio
from contextlib import contextmanager
from typing import Any

import anyio.to_thread
import httpx


async def _run_sync_inline(
    func,
    *args,
    abandon_on_cancel: bool = False,
    cancellable: bool | None = None,
    limiter=None,
):
    return func(*args)


@contextmanager
def _inline_sync_endpoints():
    original = anyio.to_thread.run_sync
    anyio.to_thread.run_sync = _run_sync_inline
    try:
        yield
    finally:
        anyio.to_thread.run_sync = original


class ASGITestClient:
    """Synchronous facade over httpx.AsyncClient + ASGITransport."""

    def __init__(self, app, base_url: str = "http://testserver") -> None:
        self.app = app
        self.base_url = base_url

    def request(self, method: str, url: str, **kwargs: Any) -> httpx.Response:
        async def _send() -> httpx.Response:
            transport = httpx.ASGITransport(app=self.app)
            async with httpx.AsyncClient(
                transport=transport,
                base_url=self.base_url,
            ) as client:
                return await client.request(method, url, **kwargs)

        with _inline_sync_endpoints():
            return asyncio.run(_send())

    def get(self, url: str, **kwargs: Any) -> httpx.Response:
        return self.request("GET", url, **kwargs)
