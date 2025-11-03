"""Compatibility shim exposing the relocated SQL governance store."""

from __future__ import annotations

from ..backend.stores import sql as _sql  # type: ignore
from ..backend.stores.sql import *  # type: ignore[F401,F403]

__all__ = getattr(_sql, "__all__", [])
