"""Compatibility shim exposing the relocated filesystem governance store."""

from __future__ import annotations

from ..backend.stores import filesystem as _filesystem  # type: ignore
from ..backend.stores.filesystem import *  # type: ignore[F401,F403]

__all__ = getattr(_filesystem, "__all__", [])
