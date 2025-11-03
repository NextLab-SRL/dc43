"""Compatibility shim exposing the relocated Delta governance store."""

from __future__ import annotations

from ..backend.stores import delta as _delta  # type: ignore
from ..backend.stores.delta import *  # type: ignore[F401,F403]

__all__ = list(getattr(_delta, "__all__", []))


def __getattr__(name: str) -> object:
    """Delegate attribute access to the relocated module."""

    try:
        return getattr(_delta, name)
    except AttributeError as exc:  # pragma: no cover - mirrors default behaviour
        raise AttributeError(name) from exc


def __dir__() -> list[str]:  # pragma: no cover - trivial passthrough
    """Combine the compatibility shim attributes with the target module ones."""

    return sorted(set(globals()) | set(dir(_delta)))
