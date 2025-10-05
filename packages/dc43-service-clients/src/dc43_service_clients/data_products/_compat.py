"""Compatibility helpers for optional data product backend dependency."""

from __future__ import annotations

try:  # pragma: no cover - exercised in environments with optional deps missing
    from dc43_service_backends.data_products import DataProductRegistrationResult
except ModuleNotFoundError:  # pragma: no cover - fallback when backends absent
    from ._types import DataProductRegistrationResult

__all__ = ["DataProductRegistrationResult"]
