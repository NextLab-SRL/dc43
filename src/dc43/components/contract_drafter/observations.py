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
    SchemaObject,
    SchemaProperty,
    Server,
)

from dc43.components.contract_validation import ValidationResult
from dc43.odcs import contract_identity
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


def _build_field_draft(
    *,
    field: SchemaProperty,
    schema_info: Mapping[str, Any] | None,
    metrics: Mapping[str, Any],
    change_log: List[Dict[str, Any]],
) -> Dict[str, Any]:
    """Return the updated field payload for the draft contract."""

    data = field.model_dump()
    name = field.name or ""

    observed_type, observed_nullable = _resolve_observed_type(schema_info, field.physicalType)
    if observed_type and observed_type != (field.physicalType or ""):
        change_log.append(
            {
                "scope": f"field:{name}",
                "kind": "type",
                "status": "updated",
                "details": {
                    "from": field.physicalType,
                    "to": observed_type,
                },
            }
    )
    data["physicalType"] = observed_type

    if schema_info is None:
        change_log.append(
            {
                "scope": f"field:{name}",
                "kind": "observation",
                "status": "missing",
                "details": {
                    "message": "Field not present in latest observations",
                },
            }
        )

    required_metric = None
    if name:
        for candidate in (
            f"violations.not_null_{name}",
            f"violations.required_{name}",
        ):
            value = metrics.get(candidate)
            if isinstance(value, (int, float)):
                required_metric = float(value)
                break

    if bool(field.required) and required_metric and required_metric > 0:
        data["required"] = False
        change_log.append(
            {
                "scope": f"field:{name}",
                "kind": "constraint",
                "constraint": "required",
                "status": "relaxed",
                "details": {
                    "reason": f"Observed {required_metric:.0f} null value(s)",
                },
            }
        )
    elif bool(field.required):
        change_log.append(
            {
                "scope": f"field:{name}",
                "kind": "constraint",
                "constraint": "required",
                "status": "kept",
                "details": {"violations": required_metric or 0},
            }
        )

    if bool(field.unique):
        unique_metric = _quality_metric_value(
            metrics=metrics,
            rule_prefix="unique",
            field_name=name,
        )
        if unique_metric and unique_metric > 0:
            data["unique"] = False
            change_log.append(
                {
                    "scope": f"field:{name}",
                    "kind": "constraint",
                    "constraint": "unique",
                    "status": "relaxed",
                    "details": {"violations": unique_metric},
                }
            )
        else:
            change_log.append(
                {
                    "scope": f"field:{name}",
                    "kind": "constraint",
                    "constraint": "unique",
                    "status": "kept",
                    "details": {"violations": unique_metric or 0},
                }
            )

    new_quality: List[Dict[str, Any]] = []
    for dq in list(field.quality or []):
        rule_info = _quality_rule_key(field, dq)
        if rule_info is None:
            new_quality.append(dq.model_dump())
            change_log.append(
                {
                    "scope": f"field:{name}",
                    "kind": "quality_rule",
                    "rule": dq.rule or "custom",
                    "status": "kept",
                    "details": {"reason": "rule not automatically evaluated"},
                }
            )
            continue

        rule_prefix, label = rule_info
        metric_value = _quality_metric_value(
            metrics=metrics,
            rule_prefix=rule_prefix,
            field_name=name,
        )

        if metric_value is None:
            new_quality.append(dq.model_dump())
            change_log.append(
                {
                    "scope": f"field:{name}",
                    "kind": "quality_rule",
                    "rule": label,
                    "status": "kept",
                    "details": {"reason": "no violations reported"},
                }
            )
            continue

        enum_extension: Optional[Tuple[List[Any], List[Any]]] = None
        if rule_prefix == "enum":
            enum_extension = _enum_extension(dq=dq, metrics=metrics, field_name=name)

        if enum_extension is not None:
            updated_values, additions = enum_extension
            payload = dq.model_dump()
            payload["mustBe"] = updated_values
            new_quality.append(payload)
            change_log.append(
                {
                    "scope": f"field:{name}",
                    "kind": "quality_rule",
                    "rule": label,
                    "status": "updated",
                    "details": {
                        "violations": metric_value or 0,
                        "added_values": additions,
                    },
                }
            )
            continue

        if metric_value and metric_value > 0:
            change_log.append(
                {
                    "scope": f"field:{name}",
                    "kind": "quality_rule",
                    "rule": label,
                    "status": "removed",
                    "details": {"violations": metric_value},
                }
            )
            continue

        new_quality.append(dq.model_dump())
        change_log.append(
            {
                "scope": f"field:{name}",
                "kind": "quality_rule",
                "rule": label,
                "status": "kept",
                "details": {"violations": 0},
            }
        )

    data["quality"] = new_quality or None

    if observed_nullable is False and not data.get("required"):
        change_log.append(
            {
                "scope": f"field:{name}",
                "kind": "observation",
                "status": "not_nullable",
                "details": {
                    "message": "Runtime reported non-nullable column despite optional contract",
                },
            }
        )

    return data


def _append_validation_feedback(
    contract: OpenDataContractStandard,
    validation: ValidationResult,
) -> None:
    """Record validation errors and warnings in the contract change log."""

    entries: List[Dict[str, Any]] = []
    for message in validation.errors:
        entries.append(
            {
                "scope": "contract",
                "kind": "validation",
                "status": "error",
                "details": {"message": message},
            }
        )
    for message in validation.warnings:
        entries.append(
            {
                "scope": "contract",
                "kind": "validation",
                "status": "warning",
                "details": {"message": message},
            }
        )

    if not entries:
        return

    props = list(contract.customProperties or [])
    existing = None
    for prop in props:
        if prop.property == "draft_change_log":
            existing = prop
            break

    if existing is not None:
        payload = list(existing.value or [])
        payload.extend(entries)
        existing.value = payload
    else:
        props.append(CustomProperty(property="draft_change_log", value=entries))

    contract.customProperties = props


def draft_from_observations(
    *,
    schema: Mapping[str, Mapping[str, Any]],
    base_contract: OpenDataContractStandard,
    metrics: Mapping[str, Any] | None = None,
    bump: str = "minor",
    dataset_id: Optional[str] = None,
    dataset_version: Optional[str] = None,
    data_format: Optional[str] = None,
    dq_feedback: Optional[Dict[str, Any]] = None,
    draft_context: Optional[Mapping[str, Any]] = None,
) -> OpenDataContractStandard:
    """Create a draft ODCS document using schema & metric observations."""

    schema_map: Dict[str, Dict[str, Any]] = {k: dict(v) for k, v in (schema or {}).items()}
    metrics_map: Dict[str, Any] = dict(metrics or {})

    change_log: List[Dict[str, Any]] = []

    schema_payloads: List[Dict[str, Any]] = []
    observed_fields = set()

    for obj in list(base_contract.schema_ or []):
        obj_data = obj.model_dump()
        prop_payloads: List[Dict[str, Any]] = []
        for field in list(obj.properties or []):
            field_payload = _build_field_draft(
                field=field,
                schema_info=schema_map.get(field.name or ""),
                metrics=metrics_map,
                change_log=change_log,
            )
            prop_payloads.append(field_payload)
            if field.name:
                observed_fields.add(field.name)
        obj_data["properties"] = prop_payloads
        schema_payloads.append(obj_data)

    remaining = [
        (name, schema_map[name])
        for name in schema_map.keys()
        if name and name not in observed_fields
    ]

    if remaining:
        target_obj: Optional[Dict[str, Any]] = None
        if schema_payloads:
            target_obj = schema_payloads[0]
        else:
            target_obj = {
                "name": base_contract.id or "dataset",
                "properties": [],
            }
            schema_payloads.append(target_obj)

        props = list(target_obj.get("properties") or [])
        for name, info in remaining:
            observed_type, observed_nullable = _resolve_observed_type(info, None)
            props.append(
                {
                    "name": name,
                    "physicalType": observed_type,
                    "required": observed_nullable is False,
                }
            )
            change_log.append(
                {
                    "scope": f"field:{name}",
                    "kind": "field",
                    "status": "added",
                    "details": {
                        "physicalType": observed_type,
                        "nullable": bool(observed_nullable),
                    },
                }
            )
        target_obj["properties"] = props

    contract_id, current_version = contract_identity(base_contract)
    semver = SemVer.parse(current_version)
    target_bump = "minor" if bump not in ("major", "patch") else bump
    bumped = semver.bump(target_bump)
    suffix = _draft_version_suffix(
        dataset_id=dataset_id,
        dataset_version=dataset_version,
        draft_context=draft_context,
    )
    base_version = str(bumped)
    new_version = f"{base_version}-{suffix}" if suffix else base_version

    custom_props = list(base_contract.customProperties or [])
    custom_props.append(CustomProperty(property="draft", value=True))
    custom_props.append(CustomProperty(property="base_version", value=current_version))
    custom_props.append(
        CustomProperty(
            property="provenance",
            value={"dataset_id": dataset_id, "dataset_version": dataset_version},
        )
    )
    pipeline_label: Optional[str] = None
    if draft_context:
        custom_props.append(
            CustomProperty(property="draft_context", value=dict(draft_context))
        )
        pipeline_label = draft_context.get("pipeline") or draft_context.get("job")
        if not pipeline_label:
            pipeline_label = draft_context.get("project") or draft_context.get("module")
    if pipeline_label:
        custom_props.append(
            CustomProperty(property="draft_pipeline", value=pipeline_label)
        )
    if dq_feedback:
        custom_props.append(CustomProperty(property="dq_feedback", value=dq_feedback))
    if metrics:
        custom_props.append(CustomProperty(property="observed_metrics", value=dict(metrics)))

    if change_log:
        custom_props.append(CustomProperty(property="draft_change_log", value=change_log))

    schema_name = contract_id
    schema_objects: List[SchemaObject] = []
    if schema_payloads:
        for payload in schema_payloads:
            schema_objects.append(SchemaObject(**payload))
        schema_name = schema_objects[0].name or contract_id
    else:
        schema_objects = [
            SchemaObject(name=schema_name, properties=[]),
        ]

    servers = base_contract.servers
    if dataset_id:
        fmt = data_format
        if not fmt and base_contract.servers:
            fmt = base_contract.servers[0].format
        if dataset_id.startswith("path:"):
            servers = [
                Server(server="local", type="filesystem", path=dataset_id[5:], format=fmt)
            ]
        elif dataset_id.startswith("table:"):
            servers = [Server(server="local", dataset=dataset_id[6:], format=fmt)]

    return OpenDataContractStandard(
        version=new_version,
        kind=base_contract.kind,
        apiVersion=base_contract.apiVersion,
        id=contract_id,
        name=base_contract.name or contract_id,
        description=base_contract.description,
        status="draft",
        schema=schema_objects,
        servers=servers,
        customProperties=custom_props,
    )


def draft_from_validation_result(
    *,
    validation: ValidationResult,
    base_contract: OpenDataContractStandard,
    bump: str = "minor",
    dataset_id: Optional[str] = None,
    dataset_version: Optional[str] = None,
    data_format: Optional[str] = None,
    dq_feedback: Optional[Mapping[str, Any]] = None,
    draft_context: Optional[Mapping[str, Any]] = None,
) -> OpenDataContractStandard:
    """Create a draft contract document from an engine validation result."""

    schema_payload: Mapping[str, Mapping[str, Any]] | None = validation.schema or None
    metrics_payload: Mapping[str, Any] | None = validation.metrics or None
    context_payload: Dict[str, Any] = dict(draft_context or {})
    if dataset_id and "dataset_id" not in context_payload:
        context_payload["dataset_id"] = dataset_id
    if dataset_version and "dataset_version" not in context_payload:
        context_payload["dataset_version"] = dataset_version
    if data_format and "data_format" not in context_payload:
        context_payload["data_format"] = data_format

    draft = draft_from_observations(
        schema=schema_payload or {},
        metrics=metrics_payload or None,
        base_contract=base_contract,
        bump=bump,
        dataset_id=dataset_id,
        dataset_version=dataset_version,
        data_format=data_format,
        dq_feedback=dict(dq_feedback) if dq_feedback else None,
        draft_context=context_payload or None,
    )

    _append_validation_feedback(draft, validation)

    return draft


__all__ = ["draft_from_observations", "draft_from_validation_result"]
