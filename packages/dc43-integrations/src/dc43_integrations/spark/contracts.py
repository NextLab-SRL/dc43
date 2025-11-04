"""Spark helpers for drafting ODCS contracts from observed data."""

from __future__ import annotations

import copy
from dataclasses import dataclass
from typing import Any, Dict, Mapping

from pyspark.sql import DataFrame

from open_data_contract_standard.model import (  # type: ignore
    CustomProperty,
    Description,
    OpenDataContractStandard,
    SchemaObject,
    SchemaProperty,
)

from dc43_core import ODCS_REQUIRED, SemVer, build_odcs, ensure_version

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


def _ensure_schema_object(
    contract: OpenDataContractStandard,
    *,
    default_name: str | None = None,
) -> SchemaObject:
    schema_objects = list(getattr(contract, "schema_", []) or [])
    if schema_objects:
        obj = schema_objects[0]
    else:
        obj = SchemaObject(name=default_name, properties=[])
        schema_objects.append(obj)
        contract.schema_ = schema_objects
    return obj


def _update_properties(
    obj: SchemaObject,
    snapshot: Mapping[str, Mapping[str, Any]],
) -> None:
    updated: list[SchemaProperty] = []
    existing = {prop.name: prop for prop in obj.properties or [] if prop.name}
    for name, metadata in snapshot.items():
        required = not bool(metadata.get("nullable", False))
        physical_type = str(
            metadata.get("odcs_type") or metadata.get("backend_type") or "string"
        )
        prop = existing.get(name)
        if prop is None:
            prop = SchemaProperty(name=name)
        prop.required = required
        prop.physicalType = physical_type
        updated.append(prop)
    obj.properties = updated


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
    document using ``contract_id``/``base_version`` so a patch version can be
    bumped and the observed schema attached.  The returned
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

    draft = copy.deepcopy(base_contract)
    ensure_version(draft)

    bump = SemVer.parse(draft.version or base_version).bump("patch")
    suffix_parts: list[str] = []
    if dataset_id:
        suffix_parts.append("dataset")
    if dataset_version:
        suffix_parts.append("version")
    if draft_context:
        suffix_parts.append("ctx")
    suffix_parts.append("draft")
    suffix = "-".join(suffix_parts)
    draft.version = f"{bump}-{suffix}" if suffix else str(bump)
    draft.status = "draft"

    schema_obj = _ensure_schema_object(draft, default_name=name or contract_id)
    _update_properties(schema_obj, schema)

    context_payload: Dict[str, object] = {}
    if draft_context:
        context_payload.update(dict(draft_context))
    if dataset_id:
        context_payload.setdefault("dataset_id", dataset_id)
    if dataset_version:
        context_payload.setdefault("dataset_version", dataset_version)
    if context_payload:
        draft.customProperties = list(draft.customProperties or [])
        draft.customProperties.append(
            CustomProperty(property="draft_context", value=context_payload)
        )

    return DraftContractResult(contract=draft, schema=schema, metrics=metrics)


__all__ = ["DraftContractResult", "draft_contract_from_dataframe"]
