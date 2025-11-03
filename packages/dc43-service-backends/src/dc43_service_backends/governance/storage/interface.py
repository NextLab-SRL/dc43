"""Compatibility shim exposing the relocated governance store interface."""

from __future__ import annotations

from ..backend.stores import interface as _interface  # type: ignore
from ..backend.stores.interface import *  # type: ignore[F401,F403]

__all__ = getattr(_interface, "__all__", [])
