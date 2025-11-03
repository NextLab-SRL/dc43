"""Compatibility shim exposing the relocated HTTP governance store."""

from __future__ import annotations

from ..backend.stores import http as _http  # type: ignore
from ..backend.stores.http import *  # type: ignore[F401,F403]

__all__ = getattr(_http, "__all__", [])
