"""Compatibility shim exposing the relocated Delta governance store."""

from __future__ import annotations

from ..backend.stores import delta as _delta  # type: ignore
from ..backend.stores.delta import *  # type: ignore[F401,F403]

__all__ = getattr(_delta, "__all__", [])
