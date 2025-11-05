"""Compatibility shim exposing the relocated HTTP governance store."""

from __future__ import annotations

from ..backend.stores import http as _http  # type: ignore
from ..backend.stores.http import *  # type: ignore[F401,F403]

try:
    __all__ = list(_http.__all__)
except AttributeError:
    __all__ = []


def __getattr__(name: str) -> object:
    """Delegate attribute access to the relocated module."""

    if name in _http.__dict__:
        return _http.__dict__[name]
    raise AttributeError(name)


def __dir__() -> list[str]:  # pragma: no cover - trivial passthrough
    """Combine the compatibility shim attributes with the target module ones."""

    return sorted(set(globals()) | set(dir(_http)))
