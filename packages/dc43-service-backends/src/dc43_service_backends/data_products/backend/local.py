"""In-memory data product backend used by the service stack."""

from __future__ import annotations

from collections import defaultdict
from pathlib import Path
from typing import Dict, Iterable, Mapping, Optional
import json
import logging

from dc43_service_clients.odps import (
    DataProductInputPort,
    DataProductOutputPort,
    OpenDataProductStandard,
    as_odps_dict,
    evolve_to_draft,
    to_model,
)

from .interface import DataProductRegistrationResult, DataProductServiceBackend


logger = logging.getLogger(__name__)


def _as_custom_properties(data: Optional[Mapping[str, object]]) -> list[dict[str, object]]:
    if not data:
        return []
    props: list[dict[str, object]] = []
    for key, value in data.items():
        props.append({"property": str(key), "value": value})
    return props


class LocalDataProductServiceBackend(DataProductServiceBackend):
    """Store ODPS documents in memory while providing port registration helpers."""

    def __init__(self) -> None:
        self._products: Dict[str, Dict[str, OpenDataProductStandard]] = defaultdict(dict)
        self._latest: Dict[str, str] = {}

    def _existing_versions(self, data_product_id: str) -> Iterable[str]:
        return self._products.get(data_product_id, {}).keys()

    def put(self, product: OpenDataProductStandard) -> None:  # noqa: D401 - short docstring
        if not product.version:
            raise ValueError("Data product version is required")
        store = self._products.setdefault(product.id, {})
        store[product.version] = product.clone()
        self._latest[product.id] = product.version

    def get(self, data_product_id: str, version: str) -> OpenDataProductStandard:  # noqa: D401
        versions = self._products.get(data_product_id)
        if not versions or version not in versions:
            raise FileNotFoundError(f"data product {data_product_id}:{version} not found")
        return versions[version].clone()

    def latest(self, data_product_id: str) -> Optional[OpenDataProductStandard]:  # noqa: D401
        version = self._latest.get(data_product_id)
        if version is None:
            return None
        return self.get(data_product_id, version)

    def list_versions(self, data_product_id: str) -> list[str]:  # noqa: D401
        versions = self._products.get(data_product_id, {})
        return sorted(versions.keys())

    def _ensure_product(self, data_product_id: str) -> OpenDataProductStandard:
        latest = self.latest(data_product_id)
        if latest is not None:
            return latest.clone()
        # Seed a minimal draft when the product does not yet exist
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
    ) -> DataProductRegistrationResult:  # noqa: D401
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
    ) -> DataProductRegistrationResult:  # noqa: D401
        product = self._ensure_product(data_product_id)
        did_change = product.ensure_output_port(port)
        if not did_change:
            return DataProductRegistrationResult(product=product, changed=False)

        props = _as_custom_properties(custom_properties)
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
    ) -> Optional[tuple[str, str]]:  # noqa: D401
        product = self.latest(data_product_id)
        if product is None:
            return None
        port = product.find_output_port(port_name)
        if port is None or not port.contract_id:
            return None
        return port.contract_id, port.version


class FilesystemDataProductServiceBackend(LocalDataProductServiceBackend):
    """Persist ODPS documents as JSON files following the ODPS schema."""

    def __init__(self, root: str | Path) -> None:
        self._root_path = Path(root)
        self._root_path.mkdir(parents=True, exist_ok=True)
        super().__init__()
        self._load_existing()

    def _product_dir(self, data_product_id: str) -> Path:
        safe_id = data_product_id.replace("/", "__")
        return self._root_path / safe_id

    def _product_path(self, data_product_id: str, version: str) -> Path:
        return self._product_dir(data_product_id) / f"{version}.json"

    def _load_existing(self) -> None:
        for json_path in self._root_path.rglob("*.json"):
            try:
                with json_path.open("r", encoding="utf-8") as handle:
                    payload = json.load(handle)
                product = to_model(payload)
            except Exception:  # pragma: no cover - defensive, best-effort loader
                logger.exception("Failed to load data product definition from %s", json_path)
                continue
            LocalDataProductServiceBackend.put(self, product)
    
    def put(self, product: OpenDataProductStandard) -> None:  # noqa: D401
        super().put(product)
        if not product.version:
            return
        path = self._product_path(product.id, product.version)
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("w", encoding="utf-8") as handle:
            json.dump(as_odps_dict(product), handle, indent=2, sort_keys=True)


__all__ = ["LocalDataProductServiceBackend", "FilesystemDataProductServiceBackend"]

