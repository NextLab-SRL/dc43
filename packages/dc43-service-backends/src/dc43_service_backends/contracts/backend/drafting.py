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

from dc43_service_backends.core.odcs import as_odcs_dict, contract_identity, normalise_custom_properties, to_model
from dc43_service_clients.data_quality import ValidationResult
from dc43_service_backends.core.versioning import SemVer


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

    if hasattr(base_contract, "model_copy"):
        draft = base_contract.model_copy(deep=True)  # type: ignore[attr-defined]
    else:
        draft = to_model(as_odcs_dict(base_contract))
    draft.version = str(bump_version)
    draft.status = "draft"

    suffix = _draft_version_suffix(
        dataset_id=dataset_id,
        dataset_version=dataset_version,
        draft_context=draft_context,
    )
    draft.version = f"{draft.version}-{suffix}"

    context_payload: Dict[str, Any] = dict(draft_context or {})
    if dataset_id and "dataset_id" not in context_payload:
        context_payload["dataset_id"] = dataset_id
    if dataset_version and "dataset_version" not in context_payload:
        context_payload["dataset_version"] = dataset_version

    pipeline_token = _pipeline_hint(draft_context)
    pipeline_value: Optional[str] = None
    if draft_context:
        for key in ("pipeline", "job", "project", "module", "function", "qualname", "source"):
            raw = draft_context.get(key)
            if raw:
                pipeline_value = str(raw)
                break

    change_log: List[Dict[str, Any]] = []
    change_log = _apply_schema_feedback(
        draft,
        schema=schema,
        metrics=metrics,
        change_log=change_log,
    )

    if validation.errors:
        change_log.append(
            {
                "status": "error",
                "kind": "validation",
                "messages": list(validation.errors),
            }
        )
    if validation.warnings:
        change_log.append(
            {
                "status": "warning",
                "kind": "validation",
                "messages": list(validation.warnings),
            }
        )

    try:
        draft_custom_properties = draft.customProperties
    except AttributeError:
        draft_custom_properties = None
    custom_properties = list(normalise_custom_properties(draft_custom_properties))

    if dq_status or dq_feedback:
        feedback = dict(dq_feedback or {})
        if dq_status:
            feedback.setdefault("status", dq_status)
        custom_properties.append(CustomProperty(property="dq_feedback", value=feedback))

    custom_properties.append(
        CustomProperty(
            property="validation_metrics",
            value={"metrics": metrics, "schema": schema},
        )
    )

    if data_format:
        custom_properties.append(CustomProperty(property="data_format", value=data_format))

    custom_properties.append(
        CustomProperty(
            property="base_contract",
            value={"id": contract_id, "version": version},
        )
    )

    custom_properties.append(
        CustomProperty(
            property="validation_outcome",
            value={"errors": validation.errors, "warnings": validation.warnings},
        )
    )

    if context_payload:
        if pipeline_value and "module" not in context_payload:
            module_hint = pipeline_value.rsplit(".", 1)[0]
            context_payload.setdefault("module", module_hint)
        custom_properties.append(
            CustomProperty(property="draft_context", value=context_payload)
        )

    if pipeline_value:
        custom_properties.append(
            CustomProperty(property="draft_pipeline", value=pipeline_value)
        )
    elif pipeline_token:
        custom_properties.append(
            CustomProperty(property="draft_pipeline", value=pipeline_token)
        )

    provenance: Dict[str, Any] = {}
    if dataset_version:
        provenance["dataset_version"] = dataset_version
    if dataset_id:
        provenance["dataset_id"] = dataset_id
    if provenance:
        custom_properties.append(
            CustomProperty(property="provenance", value=provenance)
        )

    if dataset_id or dataset_version:
        reference = {
            "dataset_id": dataset_id,
            "dataset_version": dataset_version,
            "collected_at": datetime.now(timezone.utc).isoformat(),
        }
        custom_properties.append(
            CustomProperty(property="validation_reference", value=reference)
        )

    custom_properties.append(
        CustomProperty(property="draft_change_log", value=change_log)
    )

    draft.customProperties = custom_properties

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

    if hasattr(base_contract, "model_copy"):
        draft = base_contract.model_copy(deep=True)  # type: ignore[attr-defined]
    else:
        draft = to_model(as_odcs_dict(base_contract))
    contract_id, version = contract_identity(base_contract)
    bump_version = SemVer.parse(version).bump("patch")

    suffix = _draft_version_suffix(
        dataset_id=dataset_id,
        dataset_version=dataset_version,
        draft_context=draft_context,
    )
    draft.version = f"{bump_version}-{suffix}"
    draft.status = "draft"

    context_payload: Dict[str, Any] = dict(draft_context or {})
    if dataset_id and "dataset_id" not in context_payload:
        context_payload["dataset_id"] = dataset_id
    if dataset_version and "dataset_version" not in context_payload:
        context_payload["dataset_version"] = dataset_version

    pipeline_token = _pipeline_hint(draft_context)
    pipeline_value: Optional[str] = None
    if draft_context:
        for key in ("pipeline", "job", "project", "module", "function", "qualname", "source"):
            raw = draft_context.get(key)
            if raw:
                pipeline_value = str(raw)
                break

    change_log = _apply_schema_feedback(
        draft,
        schema=observations or {},
        metrics={},
        change_log=[],
    )

    try:
        draft_custom_properties = draft.customProperties
    except AttributeError:
        draft_custom_properties = None
    custom_properties = list(normalise_custom_properties(draft_custom_properties))
    custom_properties.append(
        CustomProperty(
            property="base_contract",
            value={"id": contract_id, "version": version},
        )
    )
    custom_properties.append(
        CustomProperty(property="observed_schema", value=observations or {})
    )

    if context_payload:
        if pipeline_value and "module" not in context_payload:
            module_hint = pipeline_value.rsplit(".", 1)[0]
            context_payload.setdefault("module", module_hint)
        custom_properties.append(
            CustomProperty(property="draft_context", value=context_payload)
        )
    if pipeline_value:
        custom_properties.append(
            CustomProperty(property="draft_pipeline", value=pipeline_value)
        )
    elif pipeline_token:
        custom_properties.append(
            CustomProperty(property="draft_pipeline", value=pipeline_token)
        )
    custom_properties.append(
        CustomProperty(property="draft_change_log", value=change_log)
    )

    draft.customProperties = custom_properties

    return draft


def _apply_schema_feedback(
    draft: OpenDataContractStandard,
    *,
    schema: Mapping[str, Mapping[str, Any]],
    metrics: Mapping[str, Any],
    change_log: Optional[List[Dict[str, Any]]] = None,
) -> List[Dict[str, Any]]:
    """Update ``draft`` schema using observed field metadata."""

    log: List[Dict[str, Any]] = change_log if change_log is not None else []

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
            was_required = bool(field.required)
            if nullable is not None:
                field.required = not nullable
            if was_required and not field.required:
                log.append({
                    "field": name,
                    "status": "relaxed",
                    "constraint": "required",
                })
            if observed:
                field.description = field.description or ""
                field.description = (
                    f"{field.description}\nObserved metadata: {observed}".strip()
                )

            updated_quality: List[DataQuality] = []
            for dq in list(field.quality or []):
                result = _quality_rule_key(field, dq)
                if not result:
                    updated_quality.append(dq)
                    continue
                prefix, label = result
                value = _quality_metric_value(
                    metrics=metrics,
                    rule_prefix=prefix,
                    field_name=name,
                )
                if prefix == "enum":
                    extension = _enum_extension(dq=dq, metrics=metrics, field_name=name)
                    if extension:
                        updated, additions = extension
                        dq.mustBe = updated
                        log.append({
                            "field": name,
                            "rule": "enum",
                            "status": "updated",
                            "details": {"added_values": additions},
                        })
                    else:
                        log.append({
                            "field": name,
                            "rule": "enum",
                            "status": "kept",
                        })
                    updated_quality.append(dq)
                    continue

                if value and value > 0:
                    log.append({
                        "field": name,
                        "rule": label,
                        "status": "removed",
                        "details": {"violations": value},
                    })
                    continue

                log.append({
                    "field": name,
                    "rule": label,
                    "status": "kept",
                })
                dq.description = dq.description or ""
                dq.description = (
                    f"{dq.description}\nObserved {label}: {value}".strip()
                )
                updated_quality.append(dq)

            field.quality = updated_quality or None

    return log


__all__ = [
    "draft_from_observations",
    "draft_from_validation_result",
]
