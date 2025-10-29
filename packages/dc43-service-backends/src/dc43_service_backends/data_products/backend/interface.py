"""Protocol describing data product service operations."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Mapping, Optional, Protocol, Sequence

from dc43_service_clients.odps import (
    DataProductInputPort,
    DataProductOutputPort,
    OpenDataProductStandard,
)


@dataclass
class DataProductRegistrationResult:
    """Result returned by port registration operations."""

    product: OpenDataProductStandard
    changed: bool


@dataclass(slots=True)
class DataProductListing:
    """Paginated collection of data product identifiers."""

    items: Sequence[str]
    total: int
    limit: int | None = None
    offset: int = 0


class DataProductServiceBackend(Protocol):
    """Interface implemented by data product management backends."""

    def put(self, product: OpenDataProductStandard) -> None:
        """Persist ``product`` making it available for subsequent lookups."""

    def list_data_products(
        self, *, limit: int | None = None, offset: int = 0
    ) -> DataProductListing:
        """Return identifiers for data products tracked by the backend."""

    def get(self, data_product_id: str, version: str) -> OpenDataProductStandard:
        """Return a specific version of ``data_product_id``."""

    def latest(self, data_product_id: str) -> Optional[OpenDataProductStandard]:
        """Return the latest version of ``data_product_id`` when available."""

    def list_versions(self, data_product_id: str) -> Sequence[str]:
        """Return the known versions for ``data_product_id``."""

    def register_input_port(
        self,
        *,
        data_product_id: str,
        port: DataProductInputPort,
        bump: str = "minor",
        custom_properties: Optional[Mapping[str, object]] = None,
        source_data_product: Optional[str] = None,
        source_output_port: Optional[str] = None,
    ) -> DataProductRegistrationResult:
        """Ensure ``port`` is attached to ``data_product_id`` input ports."""

    def register_output_port(
        self,
        *,
        data_product_id: str,
        port: DataProductOutputPort,
        bump: str = "minor",
        custom_properties: Optional[Mapping[str, object]] = None,
    ) -> DataProductRegistrationResult:
        """Ensure ``port`` is attached to ``data_product_id`` output ports."""

    def resolve_output_contract(
        self,
        *,
        data_product_id: str,
        port_name: str,
    ) -> Optional[tuple[str, str]]:
        """Return ``(contract_id, contract_version)`` for ``port_name`` when known."""


__all__ = [
    "DataProductRegistrationResult",
    "DataProductListing",
    "DataProductServiceBackend",
]

