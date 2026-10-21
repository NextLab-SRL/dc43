"""Concurrency conflict detection and retry helpers for governance stores."""

from __future__ import annotations

from functools import wraps
import logging
import random
import time
from typing import Any, Callable, TypeVar

logger = logging.getLogger(__name__)

T = TypeVar("T")

DEFAULT_MAX_RETRIES = 5
DEFAULT_RETRY_BACKOFF = 0.2

_CONCURRENCY_ERROR_MARKERS: tuple[str, ...] = (
    "DELTA_CONCURRENT_DELETE_READ",
    "DELTA_CONCURRENT_DELETE_DELETE",
    "DELTA_CONCURRENT_APPEND",
    "DELTA_CONCURRENT_UPDATE",
    "DELTA_CONCURRENT_WRITE",
    "CONCURRENT_MODIFICATION",
    "ConcurrentModificationException",
    "ConcurrentAppendException",
    "ConcurrentDeleteReadException",
    "ConcurrentDeleteDeleteException",
    "ConcurrentTransactionException",
    "MetadataChangedException",
    "ProtocolChangedException",
    "Transaction conflict detected",
    "SerializationFailure",
    "could not serialize access due to concurrent",
    "database is locked",
)


def is_concurrency_error(exc: BaseException) -> bool:
    """Return ``True`` when ``exc`` indicates an optimistic concurrency conflict."""

    visited: set[int] = set()
    current: BaseException | None = exc
    while current is not None and id(current) not in visited:
        visited.add(id(current))
        type_name = type(current).__name__
        message = f"{type_name}: {current}"
        if any(marker in message for marker in _CONCURRENCY_ERROR_MARKERS):
            return True
        next_exc = getattr(current, "orig", None) or current.__cause__ or current.__context__
        current = next_exc if isinstance(next_exc, BaseException) else None
    return False


def run_with_concurrency_retry(
    operation: Callable[[], T],
    *,
    max_retries: int = DEFAULT_MAX_RETRIES,
    retry_backoff: float = DEFAULT_RETRY_BACKOFF,
    sleep_fn: Callable[[float], None] = time.sleep,
    operation_name: str = "governance store operation",
) -> T:
    """Execute ``operation`` and retry on transient concurrency conflicts."""

    retries = max(0, int(max_retries))
    base_backoff = max(0.0, float(retry_backoff))
    attempt = 0
    while True:
        try:
            return operation()
        except Exception as exc:
            if attempt >= retries or not is_concurrency_error(exc):
                raise
            attempt += 1
            delay = base_backoff * (2 ** (attempt - 1)) * random.uniform(0.5, 1.5)
            logger.warning(
                "Concurrency conflict during %s (attempt %d/%d): %s; retrying in %.3fs",
                operation_name,
                attempt,
                retries,
                exc,
                delay,
            )
            if delay > 0:
                sleep_fn(delay)


def retry_on_concurrency_conflict(method: Callable[..., T]) -> Callable[..., T]:
    """Decorator retrying store write methods on transient concurrency conflicts."""

    @wraps(method)
    def _wrapped(self: Any, *args: Any, **kwargs: Any) -> T:
        return run_with_concurrency_retry(
            lambda: method(self, *args, **kwargs),
            max_retries=getattr(self, "_max_retries", DEFAULT_MAX_RETRIES),
            retry_backoff=getattr(self, "_retry_backoff", DEFAULT_RETRY_BACKOFF),
            operation_name=f"{type(self).__name__}.{method.__name__}",
        )

    return _wrapped


__all__ = [
    "DEFAULT_MAX_RETRIES",
    "DEFAULT_RETRY_BACKOFF",
    "is_concurrency_error",
    "retry_on_concurrency_conflict",
    "run_with_concurrency_retry",
]
