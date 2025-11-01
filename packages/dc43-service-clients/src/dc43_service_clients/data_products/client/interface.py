"""Protocols for interacting with data product services."""

from __future__ import annotations

from typing import Mapping, Optional, Protocol

from dc43_service_clients.odps import OpenDataProductStandard

from .._compat import DataProductRegistrationResult


class DataProductServiceClient(Protocol):
    """Expose operations provided by the data product management service."""

    def put(self, product: OpenDataProductStandard) -> None:
        """Persist ``product`` so that it can be retrieved later."""

    def get(self, data_product_id: str, version: str) -> OpenDataProductStandard:
        ...

    def latest(self, data_product_id: str) -> Optional[OpenDataProductStandard]:
        ...

    def list_versions(self, data_product_id: str) -> list[str]:
        ...

    def list_data_products(
        self, *, limit: int | None = None, offset: int = 0
    ) -> Mapping[str, object]:
        ...

    def register_input_port(
        self,
        *,
        data_product_id: str,
        port_name: str,
        contract_id: str,
        contract_version: str,
        bump: str = "minor",
        custom_properties: Optional[Mapping[str, object]] = None,
        source_data_product: Optional[str] = None,
        source_output_port: Optional[str] = None,
    ) -> DataProductRegistrationResult:
        ...

    def register_output_port(
        self,
        *,
        data_product_id: str,
        port_name: str,
        contract_id: str,
        contract_version: str,
        bump: str = "minor",
        custom_properties: Optional[Mapping[str, object]] = None,
    ) -> DataProductRegistrationResult:
        ...

    def resolve_output_contract(
        self,
        *,
        data_product_id: str,
        port_name: str,
    ) -> Optional[tuple[str, str]]:
        ...


__all__ = ["DataProductServiceClient"]

