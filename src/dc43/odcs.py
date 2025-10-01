"""Compatibility layer for :mod:`dc43.core.odcs` utilities."""

from __future__ import annotations

from .core.odcs import *  # type: ignore[F403]
from .core.odcs import __all__ as _CORE_ALL

__all__ = list(_CORE_ALL)
