"""Compatibility shim exposing the relocated filesystem governance store."""

from __future__ import annotations

from ..backend.stores import filesystem as _filesystem  # type: ignore
from ..backend.stores.filesystem import *  # type: ignore[F401,F403]

try:
    __all__ = list(_filesystem.__all__)
except AttributeError:
    __all__ = []


def __getattr__(name: str) -> object:
    """Delegate attribute access to the relocated module."""

    if name in _filesystem.__dict__:
        return _filesystem.__dict__[name]
    raise AttributeError(name)


def __dir__() -> list[str]:  # pragma: no cover - trivial passthrough
    """Combine the compatibility shim attributes with the target module ones."""

    return sorted(set(globals()) | set(dir(_filesystem)))
