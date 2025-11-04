"""Light-weight helpers for data product integrations."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Mapping, MutableMapping, Optional


@dataclass
class DataProductInputBinding:
    """Describe how a dataset should be registered as an input port."""

    data_product: str
    port_name: Optional[str] = None
    source_data_product: Optional[str] = None
    source_output_port: Optional[str] = None
    bump: str = "minor"
    custom_properties: Optional[Mapping[str, object]] = None
    data_product_version: Optional[str] = None
    source_data_product_version: Optional[str] = None
    source_contract_version: Optional[str] = None


@dataclass
class DataProductOutputBinding:
    """Describe how a dataset should be registered as an output port."""

    data_product: str
    port_name: Optional[str] = None
    bump: str = "minor"
    custom_properties: Optional[Mapping[str, object]] = None
    data_product_version: Optional[str] = None


def _normalise_mapping(value: Optional[Mapping[str, object]]) -> Optional[Mapping[str, object]]:
    if value is None:
        return None
    if isinstance(value, MutableMapping):
        return value
    return dict(value)


def normalise_input_binding(
    spec: DataProductInputBinding | Mapping[str, object] | None,
) -> Optional[DataProductInputBinding]:
    """Convert ``spec`` to :class:`DataProductInputBinding` when possible."""

    if spec is None:
        return None
    if isinstance(spec, DataProductInputBinding):
        spec.custom_properties = _normalise_mapping(spec.custom_properties)
        return spec
    if isinstance(spec, Mapping):
        data_product = spec.get("data_product") or spec.get("dataProduct")
        if not data_product:
            return None
        version = spec.get("data_product_version") or spec.get("dataProductVersion")
        source_version = spec.get("source_data_product_version") or spec.get("sourceDataProductVersion")
        source_contract_version = spec.get("source_contract_version") or spec.get("sourceContractVersion")
        return DataProductInputBinding(
            data_product=str(data_product),
            port_name=str(spec.get("port_name") or spec.get("portName") or "").strip() or None,
            source_data_product=str(spec.get("source_data_product") or spec.get("sourceDataProduct") or "").strip() or None,
            source_output_port=str(spec.get("source_output_port") or spec.get("sourceOutputPort") or "").strip() or None,
            bump=str(spec.get("bump") or "minor"),
            custom_properties=_normalise_mapping(
                spec.get("custom_properties")
                or spec.get("customProperties")
                or None
            ),
            data_product_version=(
                str(version).strip() or None if version is not None else None
            ),
            source_data_product_version=(
                str(source_version).strip() or None if source_version is not None else None
            ),
            source_contract_version=(
                str(source_contract_version).strip() or None
                if source_contract_version is not None
                else None
            ),
        )
    return None


def normalise_output_binding(
    spec: DataProductOutputBinding | Mapping[str, object] | None,
) -> Optional[DataProductOutputBinding]:
    """Convert ``spec`` to :class:`DataProductOutputBinding` when possible."""

    if spec is None:
        return None
    if isinstance(spec, DataProductOutputBinding):
        spec.custom_properties = _normalise_mapping(spec.custom_properties)
        return spec
    if isinstance(spec, Mapping):
        data_product = spec.get("data_product") or spec.get("dataProduct")
        if not data_product:
            return None
        version = spec.get("data_product_version") or spec.get("dataProductVersion")
        return DataProductOutputBinding(
            data_product=str(data_product),
            port_name=str(spec.get("port_name") or spec.get("portName") or "").strip() or None,
            bump=str(spec.get("bump") or "minor"),
            custom_properties=_normalise_mapping(
                spec.get("custom_properties")
                or spec.get("customProperties")
                or None
            ),
            data_product_version=(
                str(version).strip() or None if version is not None else None
            ),
        )
    return None


__all__ = [
    "DataProductInputBinding",
    "DataProductOutputBinding",
    "normalise_input_binding",
    "normalise_output_binding",
]

