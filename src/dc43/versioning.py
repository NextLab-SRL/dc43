"""Compatibility layer for :mod:`dc43.core.versioning` utilities."""

from __future__ import annotations

from .core.versioning import *  # type: ignore[F403]
from .core.versioning import __all__ as _CORE_ALL

__all__ = list(_CORE_ALL)
