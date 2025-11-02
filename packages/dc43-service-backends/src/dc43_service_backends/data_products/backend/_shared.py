"""Shared helpers for mutable data product backends."""

from __future__ import annotations

from typing import Iterable, Mapping, Optional

from dc43_service_clients.odps import (
    DataProductInputPort,
    DataProductOutputPort,
    OpenDataProductStandard,
    evolve_to_draft,
)

from .interface import DataProductRegistrationResult
from dc43_service_backends.core.versioning import version_key


def _as_custom_properties(data: Optional[Mapping[str, object]]) -> list[dict[str, object]]:
    if not data:
        return []
    props: list[dict[str, object]] = []
    for key, value in data.items():
        props.append({"property": str(key), "value": value})
    return props


def _merge_custom_properties(
    port: DataProductInputPort | DataProductOutputPort,
    values: list[dict[str, object]],
) -> None:
    if not values:
        return
    existing = list(getattr(port, "custom_properties", []))
    for item in values:
        if item not in existing:
            existing.append(item)
    port.custom_properties = existing  # type: ignore[attr-defined]


def _version_key(version: str) -> tuple[int, int, int, int, int, int]:
    return version_key(version)


class MutableDataProductBackendMixin:
    """Provide port registration helpers for mutable backends."""

    def _existing_versions(self, data_product_id: str) -> Iterable[str]:
        return self.list_versions(data_product_id)

    def _ensure_product(self, data_product_id: str) -> OpenDataProductStandard:
        product = self.latest(data_product_id)
        if product is not None:
            return product.clone()
        draft = OpenDataProductStandard(id=data_product_id, status="draft")
        draft.version = None
        return draft

    def _store_updated(
        self,
        product: OpenDataProductStandard,
        *,
        data_product_id: str,
        bump: str,
    ) -> OpenDataProductStandard:
        evolve_to_draft(
            product,
            existing_versions=self._existing_versions(data_product_id),
            bump=bump,
        )
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

        props = _as_custom_properties(custom_properties)
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
        _merge_custom_properties(port, props)

        updated = self._store_updated(
            product,
            data_product_id=data_product_id,
            bump=bump,
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

        props = _as_custom_properties(custom_properties)
        _merge_custom_properties(port, props)

        updated = self._store_updated(
            product,
            data_product_id=data_product_id,
            bump=bump,
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


__all__ = [
    "MutableDataProductBackendMixin",
    "_as_custom_properties",
    "_merge_custom_properties",
    "_version_key",
]

