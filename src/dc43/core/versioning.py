"""Compatibility shim re-exporting SemVer helpers from dc43-service-backends."""

from __future__ import annotations

from dc43_service_backends.core.versioning import *  # noqa: F401,F403
from dc43_service_backends.core.versioning import __all__ as _BACKEND_ALL

__all__ = list(_BACKEND_ALL)
