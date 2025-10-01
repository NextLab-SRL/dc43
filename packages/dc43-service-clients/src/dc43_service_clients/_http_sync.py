"""Utilities for interacting with httpx clients from synchronous code."""

from __future__ import annotations

import asyncio
import inspect
import threading
from typing import Any, Awaitable, TypeVar

import httpx

T = TypeVar("T")

_LOOP_LOCAL = threading.local()
def _ensure_thread_loop() -> asyncio.AbstractEventLoop:
    """Return an event loop bound to the current thread."""

    loop = getattr(_LOOP_LOCAL, "loop", None)
    if loop is None or loop.is_closed():
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        _LOOP_LOCAL.loop = loop
    return loop


def _await_sync(awaitable: Awaitable[T]) -> T:
    """Wait for ``awaitable`` from synchronous code.

    The helper prefers ``asyncio.run`` when no loop is running in the current
    thread. If a loop is already running we raise a descriptive error instead of
    deadlocking the application. In that scenario callers should run the client
    call in a worker thread (for example via ``asyncio.to_thread``) or switch to
    an async-aware implementation.
    """

    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:  # No running loop, safe to block.
        loop = _ensure_thread_loop()
        if loop.is_running():  # pragma: no cover - defensive guard
            raise RuntimeError("Thread event loop is already running")
        return loop.run_until_complete(awaitable)

    raise RuntimeError(
        "Cannot synchronously wait on an asynchronous httpx client while an "
        "event loop is running. Execute the call in a worker thread or use an "
        "async-aware service client."
    )


def ensure_response(result: Any) -> "httpx.Response":
    """Return an ``httpx.Response`` from ``result``.

    ``result`` may already be a response (for synchronous clients) or an
    awaitable (for ``httpx.AsyncClient``). In the latter case we synchronously
    wait for the response.
    """

    if inspect.isawaitable(result):
        result = _await_sync(result)
    if not isinstance(result, httpx.Response):  # pragma: no cover - safety net
        raise TypeError(f"Expected httpx.Response, received {type(result)!r}")
    return result


def close_client(client: Any) -> None:
    """Best-effort close for sync and async httpx clients."""

    close = getattr(client, "close", None)
    if callable(close):
        maybe_awaitable = close()
        if inspect.isawaitable(maybe_awaitable):
            _await_sync(maybe_awaitable)
        return
    aclose = getattr(client, "aclose", None)
    if callable(aclose):
        _await_sync(aclose())


__all__ = ["ensure_response", "close_client"]

