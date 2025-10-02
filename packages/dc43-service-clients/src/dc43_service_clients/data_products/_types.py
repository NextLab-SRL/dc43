"""Internal fallbacks for data product service types."""

from __future__ import annotations

from dataclasses import dataclass

from dc43_service_clients.odps import OpenDataProductStandard


@dataclass
class DataProductRegistrationResult:
    """Result returned by port registration operations."""

    product: OpenDataProductStandard
    changed: bool


__all__ = ["DataProductRegistrationResult"]
