"""Spark helpers for drafting ODCS contracts from observed data."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Mapping

from pyspark.sql import DataFrame

from open_data_contract_standard.model import (  # type: ignore
    OpenDataContractStandard,
    SchemaObject,
    SchemaProperty,
)

from dc43_service_backends.contracts.backend.drafting import draft_from_observations
from dc43_service_backends.core.odcs import ODCS_REQUIRED, build_odcs, ensure_version
from dc43_integrations.spark.data_quality import collect_observations, schema_snapshot


@dataclass(slots=True)
class DraftContractResult:
    """Container bundling a drafted contract with the collected observations."""

    contract: OpenDataContractStandard
    schema: Dict[str, Dict[str, Any]]
    metrics: Dict[str, Any]


def _properties_from_snapshot(
    snapshot: Mapping[str, Mapping[str, Any]],
) -> list[SchemaProperty]:
    properties: list[SchemaProperty] = []
    for name, metadata in snapshot.items():
        required = not bool(metadata.get("nullable", False))
        physical_type = (
            str(metadata.get("odcs_type") or metadata.get("backend_type") or "string")
        )
        properties.append(
            SchemaProperty(
                name=name,
                physicalType=physical_type,
                required=required,
            )
        )
    return properties


def draft_contract_from_dataframe(
    df: DataFrame,
    *,
    contract_id: str | None = None,
    base_contract: OpenDataContractStandard | None = None,
    base_version: str = "0.1.0",
    dataset_id: str | None = None,
    dataset_version: str | None = None,
    draft_context: Mapping[str, object] | None = None,
    name: str | None = None,
    description: str | None = None,
    collect_metrics: bool = False,
) -> DraftContractResult:
    """Return a draft contract derived from the schema observed in ``df``.

    When ``base_contract`` is omitted the helper will materialise a minimal ODCS
    document using ``contract_id``/``base_version`` so the drafter can bump a
    patch version and attach the observed schema.  The returned
    :class:`DraftContractResult` exposes the generated contract alongside the
    schema and metrics payloads so callers can persist the observations.
    """

    if base_contract is None and not contract_id:
        raise ValueError("contract_id is required when base_contract is not provided")

    snapshot = schema_snapshot(df)
    if base_contract is None:
        properties = _properties_from_snapshot(snapshot)
        base_contract = build_odcs(
            contract_id=contract_id or "generated",
            version=base_version,
            kind="DataContract",
            api_version=ODCS_REQUIRED,
            name=name or contract_id,
            description=description,
            schema_objects=[SchemaObject(name=name or contract_id, properties=properties)],
        )
    else:
        ensure_version(base_contract)
        contract_id = contract_id or base_contract.id

    schema: Dict[str, Dict[str, Any]]
    metrics: Dict[str, Any]
    if collect_metrics:
        observed_schema, observed_metrics = collect_observations(
            df,
            base_contract,
            collect_metrics=True,
        )
        schema = {k: dict(v) for k, v in observed_schema.items()}
        metrics = {k: v for k, v in observed_metrics.items()}
    else:
        schema = {k: dict(v) for k, v in snapshot.items()}
        metrics = {}

    draft = draft_from_observations(
        observations=schema,
        base_contract=base_contract,
        dataset_id=dataset_id,
        dataset_version=dataset_version,
        draft_context=draft_context,
    )

    return DraftContractResult(contract=draft, schema=schema, metrics=metrics)


__all__ = ["DraftContractResult", "draft_contract_from_dataframe"]
