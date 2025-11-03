"""Compatibility shim exposing the relocated in-memory governance store."""

from __future__ import annotations

from ..backend.stores import memory as _memory  # type: ignore
from ..backend.stores.memory import *  # type: ignore[F401,F403]

__all__ = getattr(_memory, "__all__", [])
