"""Helpers to generate ODCS drafts from runtime observations."""

from __future__ import annotations

from datetime import datetime, timezone
import re
from typing import Any, Dict, Iterable, List, Mapping, Optional, Tuple
from uuid import uuid4

from open_data_contract_standard.model import (  # type: ignore
    CustomProperty,
    DataQuality,
    OpenDataContractStandard,
    SchemaProperty,
    Server,
)

from dc43.odcs import contract_identity
from dc43.services.data_quality.backend.engine import ValidationResult
from dc43.versioning import SemVer


_INVALID_IDENTIFIER = re.compile(r"[^0-9A-Za-z-]+")


def _normalise_identifier(value: str | None) -> Optional[str]:
    """Return a semver-friendly identifier derived from ``value``."""

    if value is None:
        return None
    token = _INVALID_IDENTIFIER.sub("-", str(value)).strip("-")
    return token or None


def _pipeline_hint(context: Mapping[str, Any] | None) -> Optional[str]:
    """Return a reviewer friendly label describing the draft origin."""

    if not context:
        return None

    for key in ("pipeline", "job", "project", "module", "function", "qualname", "source"):
        value = context.get(key)
        if value:
            token = _normalise_identifier(str(value))
            if token:
                return token
    return None


def _draft_version_suffix(
    *,
    dataset_id: Optional[str],
    dataset_version: Optional[str],
    draft_context: Optional[Mapping[str, Any]],
) -> str:
    """Return the pre-release suffix used to guarantee draft version uniqueness."""

    tokens: List[str] = ["draft"]

    for candidate in (dataset_version, dataset_id):
        token = _normalise_identifier(candidate)
        if token:
            tokens.append(token)

    pipeline_token = _pipeline_hint(draft_context)
    if pipeline_token:
        tokens.append(pipeline_token)

    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%fZ")
    tokens.append(timestamp)

    entropy = uuid4().hex[:8]
    tokens.append(entropy)

    return "-".join(tokens)


def _resolve_observed_type(
    info: Mapping[str, Any] | None,
    fallback: str | None,
) -> Tuple[str, Optional[bool]]:
    """Return the preferred ODCS physical type and nullable flag."""

    observed_type = str(
        (info or {}).get("odcs_type")
        or (info or {}).get("type")
        or (info or {}).get("backend_type")
        or fallback
        or "string"
    )
    nullable = None
    if info is not None and "nullable" in info:
        nullable = bool(info.get("nullable", False))
    return observed_type, nullable


def _quality_rule_key(field: SchemaProperty, dq: DataQuality) -> Optional[Tuple[str, str]]:
    """Return the expectation rule prefix and human readable label."""

    name = field.name or ""
    if not name:
        return None

    if dq.mustBeGreaterThan is not None:
        return "gt", f"mustBeGreaterThan {dq.mustBeGreaterThan}"
    if dq.mustBeGreaterOrEqualTo is not None:
        return "ge", f"mustBeGreaterOrEqualTo {dq.mustBeGreaterOrEqualTo}"
    if dq.mustBeLessThan is not None:
        return "lt", f"mustBeLessThan {dq.mustBeLessThan}"
    if dq.mustBeLessOrEqualTo is not None:
        return "le", f"mustBeLessOrEqualTo {dq.mustBeLessOrEqualTo}"

    rule = (dq.rule or "").lower()
    if rule == "unique":
        return "unique", "unique"
    if rule == "enum" and isinstance(dq.mustBe, Iterable):
        return "enum", "enum"
    if rule == "regex" and dq.mustBe:
        return "regex", "regex"

    return None


def _quality_metric_value(
    *,
    metrics: Mapping[str, Any],
    rule_prefix: str,
    field_name: str,
) -> Optional[float]:
    key = f"violations.{rule_prefix}_{field_name}"
    value = metrics.get(key)
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value)
    return None


def _extract_values(candidate: Any) -> List[Any]:
    """Normalise different iterable payloads into a flat list of values."""

    if candidate is None:
        return []
    if isinstance(candidate, Mapping):
        values: List[Any] = []
        for key in ("new", "new_values", "unexpected", "unexpected_values", "values", "items"):
            inner = candidate.get(key)
            if isinstance(inner, (list, tuple, set)):
                values.extend(inner)
            elif inner is not None:
                values.append(inner)
        return values
    if isinstance(candidate, (list, tuple, set)):
        return list(candidate)
    return [candidate]


def _enum_extension(
    *,
    dq: DataQuality,
    metrics: Mapping[str, Any],
    field_name: str,
) -> Optional[Tuple[List[Any], List[Any]]]:
    """Return updated enum values plus additions derived from observations."""

    if not field_name:
        return None
    base_values: List[Any]
    if isinstance(dq.mustBe, (list, tuple, set)):
        base_values = list(dq.mustBe)
    else:
        return None

    observed_sources = [
        metrics.get(f"observed.enum_{field_name}"),
        metrics.get("observed.enum", {}),
    ]
    observed_values: List[Any] = []
    for source in observed_sources:
        if isinstance(source, Mapping) and field_name in source:
            observed_values.extend(_extract_values(source.get(field_name)))
        else:
            observed_values.extend(_extract_values(source))

    if not observed_values:
        return None

    seen = {str(v) for v in base_values}
    additions: List[Any] = []
    for value in observed_values:
        key = str(value)
        if key not in seen:
            additions.append(value)
            seen.add(key)

    if not additions:
        return None

    updated = list(base_values) + additions

    return updated, additions


def draft_from_validation_result(
    *,
    validation: ValidationResult,
    base_contract: OpenDataContractStandard,
    bump: str = "minor",
    dataset_id: Optional[str] = None,
    dataset_version: Optional[str] = None,
    data_format: Optional[str] = None,
    dq_status: Optional[str] = None,
    dq_feedback: Optional[Mapping[str, Any]] = None,
    draft_context: Optional[Mapping[str, Any]] = None,
) -> Optional[OpenDataContractStandard]:
    """Return a draft contract derived from validation feedback."""

    metrics = validation.metrics or {}
    schema = validation.schema or {}

    has_errors = bool(validation.errors)
    has_warnings = bool(validation.warnings)
    if not has_errors and not has_warnings:
        return None

    contract_id, version = contract_identity(base_contract)
    bump_version = SemVer.parse(version).bump(bump)

    draft = OpenDataContractStandard.from_dict(base_contract.to_dict())
    draft.version = str(bump_version)
    draft.status = "draft"

    suffix = _draft_version_suffix(
        dataset_id=dataset_id,
        dataset_version=dataset_version,
        draft_context=draft_context,
    )
    draft.version = f"{draft.version}-{suffix}"

    draft.metadata = draft.metadata or {}
    draft.metadata.contract = draft.metadata.contract or {}
    draft.metadata.contract.references = draft.metadata.contract.references or []
    draft.metadata.contract.references.append({
        "type": "validation",
        "id": dataset_id,
        "version": dataset_version,
        "collected_at": datetime.now(timezone.utc).isoformat(),
    })

    if dq_status or dq_feedback:
        feedback = dq_feedback or {}
        feedback = dict(feedback)
        if dq_status:
            feedback.setdefault("status", dq_status)
        draft.metadata.contract.customProperties = draft.metadata.contract.customProperties or []
        draft.metadata.contract.customProperties.append(
            CustomProperty(key="dq_feedback", value=feedback)
        )

    draft.metadata.contract.customProperties = draft.metadata.contract.customProperties or []
    draft.metadata.contract.customProperties.append(
        CustomProperty(
            key="validation_metrics",
            value={"metrics": metrics, "schema": schema},
        )
    )

    if data_format:
        draft.metadata.contract.customProperties.append(
            CustomProperty(key="data_format", value=data_format)
        )

    draft.metadata.contract.customProperties.append(
        CustomProperty(key="base_contract", value={"id": contract_id, "version": version})
    )

    draft.metadata.contract.customProperties.append(
        CustomProperty(key="validation_outcome", value={
            "errors": validation.errors,
            "warnings": validation.warnings,
        })
    )

    if dataset_id:
        draft.metadata.contract.customProperties.append(
            CustomProperty(key="dataset_id", value=dataset_id)
        )
    if dataset_version:
        draft.metadata.contract.customProperties.append(
            CustomProperty(key="dataset_version", value=dataset_version)
        )

    draft.servers = draft.servers or []
    if dataset_id or dataset_version:
        draft.servers.append(
            Server(
                id=f"dataset::{dataset_id or 'unknown'}::{dataset_version or 'unknown'}",
                type="dataset",
                description="Derived from validation observations",
            )
        )

    _apply_schema_feedback(draft, schema=schema, metrics=metrics)
    return draft


def draft_from_observations(
    *,
    observations: Mapping[str, Mapping[str, Any]] | None,
    base_contract: OpenDataContractStandard,
    dataset_id: Optional[str] = None,
    dataset_version: Optional[str] = None,
    draft_context: Optional[Mapping[str, Any]] = None,
) -> OpenDataContractStandard:
    """Return a draft contract using observed schema information only."""

    draft = OpenDataContractStandard.from_dict(base_contract.to_dict())
    contract_id, version = contract_identity(base_contract)
    bump_version = SemVer.parse(version).bump("patch")

    suffix = _draft_version_suffix(
        dataset_id=dataset_id,
        dataset_version=dataset_version,
        draft_context=draft_context,
    )
    draft.version = f"{bump_version}-{suffix}"
    draft.status = "draft"

    draft.metadata = draft.metadata or {}
    draft.metadata.contract = draft.metadata.contract or {}
    draft.metadata.contract.customProperties = draft.metadata.contract.customProperties or []
    draft.metadata.contract.customProperties.append(
        CustomProperty(key="base_contract", value={"id": contract_id, "version": version})
    )
    draft.metadata.contract.customProperties.append(
        CustomProperty(key="observed_schema", value=observations or {})
    )

    _apply_schema_feedback(draft, schema=observations or {}, metrics={})
    return draft


def _apply_schema_feedback(
    draft: OpenDataContractStandard,
    *,
    schema: Mapping[str, Mapping[str, Any]],
    metrics: Mapping[str, Any],
) -> None:
    """Update ``draft`` schema using observed field metadata."""

    for obj in draft.schema_ or []:
        for field in obj.properties or []:
            name = field.name
            if not name:
                continue
            observed = schema.get(name) or {}
            observed_type, nullable = _resolve_observed_type(
                observed,
                field.physicalType or field.logicalType,
            )
            if observed_type:
                field.physicalType = observed_type
            if nullable is not None:
                field.required = not nullable
            if observed:
                field.description = field.description or ""
                field.description = (
                    f"{field.description}\nObserved metadata: {observed}".strip()
                )

            for dq in field.quality or []:
                result = _quality_rule_key(field, dq)
                if not result:
                    continue
                prefix, label = result
                value = _quality_metric_value(
                    metrics=metrics,
                    rule_prefix=prefix,
                    field_name=name,
                )
                if value is None:
                    continue
                dq.description = dq.description or ""
                dq.description = (
                    f"{dq.description}\nObserved {label}: {value}".strip()
                )


__all__ = [
    "draft_from_observations",
    "draft_from_validation_result",
]
