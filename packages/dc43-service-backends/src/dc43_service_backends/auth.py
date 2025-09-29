"""Authentication utilities for the HTTP service layer."""

from __future__ import annotations

import inspect
from collections.abc import Awaitable, Callable, Iterable
from typing import Sequence

try:  # pragma: no cover - optional dependency guard
    from fastapi import Depends, HTTPException, status
    from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
except ModuleNotFoundError as exc:  # pragma: no cover
    raise ModuleNotFoundError(
        "FastAPI is required to configure authentication. Install "
        "'dc43-service-backends[http]' to use these helpers."
    ) from exc

Verifier = Callable[[str], bool | Awaitable[bool]]


def _ensure_iterable(tokens: str | Iterable[str]) -> Sequence[str]:
    if isinstance(tokens, str):
        return (tokens,)
    return tuple(tokens)


def bearer_token_dependency(
    tokens: str | Iterable[str] | None = None,
    *,
    verifier: Verifier | None = None,
) -> Depends:
    """Return a FastAPI dependency enforcing HTTP bearer authentication.

    Parameters
    ----------
    tokens:
        A single token or collection of tokens accepted by the service.
    verifier:
        Optional callable invoked with the presented token to determine
        acceptance. The callable may be synchronous or asynchronous and should
        return ``True`` for valid credentials.

    Returns
    -------
    fastapi.Depends
        A dependency that can be supplied to ``APIRouter`` to require
        authentication for all endpoints.
    """

    if tokens is None and verifier is None:
        raise ValueError("Provide at least one token or a verifier for authentication")

    allowed_tokens: Sequence[str] = () if tokens is None else _ensure_iterable(tokens)
    scheme = HTTPBearer(auto_error=True)

    async def _verify(
        credentials: HTTPAuthorizationCredentials = Depends(scheme),
    ) -> None:
        token = credentials.credentials
        if verifier is not None:
            result = verifier(token)
            if inspect.isawaitable(result):
                result = await result
            if result:
                return
        if token in allowed_tokens:
            return
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid authentication credentials",
        )

    return Depends(_verify)


__all__ = ["bearer_token_dependency"]
