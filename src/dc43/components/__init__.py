"""Compatibility shim that proxies to :mod:`dc43.lib.components`."""
from __future__ import annotations

import importlib
from typing import Any, Iterable

_lib_components = importlib.import_module("dc43.lib.components")

# Expose the same package search path so submodules continue to resolve when
# imported via the legacy location (e.g. ``dc43.components.data_quality``).
__path__ = _lib_components.__path__  # type: ignore[attr-defined]


def __getattr__(name: str) -> Any:
    return getattr(_lib_components, name)


def __dir__() -> Iterable[str]:
    return getattr(_lib_components, "__all__", [])


from dc43.lib.components import *  # noqa: F401,F403 - re-export public API

__all__ = getattr(_lib_components, "__all__", [])
