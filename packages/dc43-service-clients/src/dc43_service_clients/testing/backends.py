"""Test-oriented data product backend implementations."""

from __future__ import annotations

from collections import defaultdict
from typing import Dict, Iterable, Mapping, Optional

from dc43_service_clients.odps import (
    DataProductInputPort,
    DataProductOutputPort,
    OpenDataProductStandard,
    evolve_to_draft,
)

from ..data_products._compat import DataProductRegistrationResult


class LocalDataProductServiceBackend:
    """Minimal in-memory backend suitable for client test suites."""

    def __init__(self) -> None:
        self._products: Dict[str, Dict[str, OpenDataProductStandard]] = defaultdict(dict)
        self._latest: Dict[str, str] = {}

    def _existing_versions(self, data_product_id: str) -> Iterable[str]:
        return self._products.get(data_product_id, {}).keys()

    def put(self, product: OpenDataProductStandard) -> None:
        if not product.version:
            raise ValueError("Data product version is required")
        store = self._products.setdefault(product.id, {})
        store[product.version] = product.clone()
        self._latest[product.id] = product.version

    def get(self, data_product_id: str, version: str) -> OpenDataProductStandard:
        versions = self._products.get(data_product_id)
        if not versions or version not in versions:
            raise FileNotFoundError(f"data product {data_product_id}:{version} not found")
        return versions[version].clone()

    def latest(self, data_product_id: str) -> Optional[OpenDataProductStandard]:
        version = self._latest.get(data_product_id)
        if version is None:
            return None
        return self.get(data_product_id, version)

    def list_versions(self, data_product_id: str) -> list[str]:
        versions = self._products.get(data_product_id, {})
        return sorted(versions.keys())

    def _ensure_product(self, data_product_id: str) -> OpenDataProductStandard:
        latest = self.latest(data_product_id)
        if latest is not None:
            return latest.clone()
        product = OpenDataProductStandard(id=data_product_id, status="draft")
        product.version = None
        return product

    def _store_updated(
        self,
        product: OpenDataProductStandard,
        *,
        bump: str,
        existing_versions: Iterable[str],
    ) -> OpenDataProductStandard:
        evolve_to_draft(product, existing_versions=existing_versions, bump=bump)
        self.put(product)
        return product

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
        product = self._ensure_product(data_product_id)
        did_change = product.ensure_input_port(port)
        if not did_change:
            return DataProductRegistrationResult(product=product, changed=False)

        props = self._as_custom_properties(custom_properties)
        if source_data_product:
            props.append(
                {
                    "property": "dc43.input.source_data_product",
                    "value": source_data_product,
                }
            )
        if source_output_port:
            props.append(
                {
                    "property": "dc43.input.source_output_port",
                    "value": source_output_port,
                }
            )
        if props:
            port.custom_properties.extend(
                [item for item in props if item not in port.custom_properties]
            )

        updated = self._store_updated(
            product,
            bump=bump,
            existing_versions=self._existing_versions(data_product_id),
        )
        return DataProductRegistrationResult(product=updated, changed=True)

    def register_output_port(
        self,
        *,
        data_product_id: str,
        port: DataProductOutputPort,
        bump: str = "minor",
        custom_properties: Optional[Mapping[str, object]] = None,
    ) -> DataProductRegistrationResult:
        product = self._ensure_product(data_product_id)
        did_change = product.ensure_output_port(port)
        if not did_change:
            return DataProductRegistrationResult(product=product, changed=False)

        props = self._as_custom_properties(custom_properties)
        if props:
            port.custom_properties.extend(
                [item for item in props if item not in port.custom_properties]
            )

        updated = self._store_updated(
            product,
            bump=bump,
            existing_versions=self._existing_versions(data_product_id),
        )
        return DataProductRegistrationResult(product=updated, changed=True)

    def resolve_output_contract(
        self,
        *,
        data_product_id: str,
        port_name: str,
    ) -> Optional[tuple[str, str]]:
        product = self.latest(data_product_id)
        if product is None:
            return None
        port = product.find_output_port(port_name)
        if port is None or not port.contract_id:
            return None
        return port.contract_id, port.version

    @staticmethod
    def _as_custom_properties(
        data: Optional[Mapping[str, object]]
    ) -> list[dict[str, object]]:
        if not data:
            return []
        props: list[dict[str, object]] = []
        for key, value in data.items():
            props.append({"property": str(key), "value": value})
        return props


__all__ = [
    "DataProductRegistrationResult",
    "LocalDataProductServiceBackend",
]
