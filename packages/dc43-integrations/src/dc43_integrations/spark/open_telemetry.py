from __future__ import annotations

"""Helpers for recording OpenTelemetry spans for governance interactions."""

from dataclasses import asdict, is_dataclass
import json
import logging
from functools import lru_cache
from typing import Any, Mapping, Optional, Sequence, Union

from dc43_service_clients.data_products import (
    DataProductInputBinding,
    DataProductOutputBinding,
)
from dc43_service_clients.data_quality import ValidationResult
from dc43_service_clients.governance import PipelineContext
from dc43_service_clients.governance.models import ResolvedReadPlan, ResolvedWritePlan

from .open_data_lineage import (
    _binding_to_dict,
    _drop_empty,
    _merge_contexts,
    _normalise_value,
    _resolve_binding,
    _resolve_contract_id,
    _resolve_contract_version,
    _resolve_dataset_id,
    _resolve_dataset_version,
    _resolve_expectation_plan,
    _resolve_format,
    _serialise_validation,
)

PipelineContextLike = Union[
    PipelineContext,
    Mapping[str, object],
    Sequence[tuple[str, object]],
    str,
]

_LineagePlan = Union[ResolvedReadPlan, ResolvedWritePlan]
_Binding = Union[DataProductInputBinding, DataProductOutputBinding]

logger = logging.getLogger(__name__)

_TRACER_NAME = "dc43.integrations.governance"
_ATTRIBUTE_PREFIX = "dc43.governance"
_VALIDATION_EVENT = "dc43.validation"
_EXPECTATION_EVENT = "dc43.expectations"


@lru_cache(maxsize=1)
def _resolve_tracer():
    try:  # pragma: no cover - exercised when OpenTelemetry is available
        from opentelemetry import trace
        from opentelemetry.trace import SpanKind
    except ImportError:  # pragma: no cover - optional dependency
        logger.debug("OpenTelemetry is not installed; governance telemetry spans will be skipped")
        return None, None
    return trace.get_tracer(_TRACER_NAME), SpanKind


def _json_default(value: Any) -> Any:
    if isinstance(value, ValidationResult):
        return _serialise_validation(value)
    if is_dataclass(value):
        return {key: _json_default(item) for key, item in asdict(value).items()}
    if isinstance(value, Mapping):
        return {str(key): _json_default(item) for key, item in value.items()}
    if isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
        return [_json_default(item) for item in value]
    return str(value)


def _encode_payload(value: Any) -> str:
    return json.dumps(value, default=_json_default, sort_keys=True)


def _attribute_value(value: Any) -> Any:
    normalised = _normalise_value(value)
    if normalised is None:
        return None
    if isinstance(normalised, (str, bool, int, float)):
        return normalised
    try:
        return _encode_payload(normalised)
    except (TypeError, ValueError):
        return str(normalised)


def _flatten_context(context: Mapping[str, Any]) -> Mapping[str, Any]:
    flattened: dict[str, Any] = {}
    for key, value in context.items():
        attribute = _attribute_value(value)
        if attribute is None:
            continue
        flattened[str(key)] = attribute
    return flattened


def _binding_payload(binding: Optional[_Binding]) -> Optional[Mapping[str, Any]]:
    resolved = _binding_to_dict(binding)
    if resolved is None:
        return None
    return _drop_empty(resolved)


def _validation_event_payload(result: ValidationResult) -> Mapping[str, Any]:
    payload = {
        "status": result.status or "unknown",
        "ok": bool(result.ok),
        "reason": result.reason or "",
        "errors_count": len(result.errors or []),
        "warnings_count": len(result.warnings or []),
        "details": _attribute_value(_serialise_validation(result)),
    }
    if not payload["reason"]:
        payload.pop("reason")
    return {key: value for key, value in payload.items() if value is not None}


def record_telemetry_span(
    *,
    operation: str,
    plan: Optional[_LineagePlan],
    pipeline_context: Optional[PipelineContextLike],
    contract_id: Optional[str],
    contract_version: Optional[str],
    dataset_id: Optional[str],
    dataset_version: Optional[str],
    dataset_format: Optional[str] = None,
    table: Optional[str] = None,
    path: Optional[str] = None,
    binding: Optional[_Binding] = None,
    validation: Optional[ValidationResult] = None,
    status: Optional[ValidationResult] = None,
    expectation_plan: Optional[Sequence[Mapping[str, Any]]] = None,
) -> None:
    tracer, span_kind = _resolve_tracer()
    if tracer is None or span_kind is None:
        return

    operation = (operation or "run").strip().lower() or "run"
    context = _merge_contexts(plan, pipeline_context)
    resolved_contract_id = _resolve_contract_id(plan, contract_id)
    resolved_contract_version = _resolve_contract_version(plan, contract_version)
    resolved_dataset_id = _resolve_dataset_id(plan, dataset_id)
    resolved_dataset_version = _resolve_dataset_version(plan, dataset_version)
    resolved_format = _resolve_format(plan, dataset_format)
    resolved_binding = _resolve_binding(plan, binding)
    resolved_expectations = _resolve_expectation_plan(expectation_plan, validation)

    span_name = f"{_TRACER_NAME}.{operation}"
    with tracer.start_as_current_span(span_name, kind=span_kind.INTERNAL) as span:
        span.set_attribute(f"{_ATTRIBUTE_PREFIX}.operation", operation)
        if resolved_contract_id:
            span.set_attribute(f"{_ATTRIBUTE_PREFIX}.contract.id", resolved_contract_id)
        if resolved_contract_version:
            span.set_attribute(
                f"{_ATTRIBUTE_PREFIX}.contract.version", resolved_contract_version
            )
        if resolved_dataset_id:
            span.set_attribute(f"{_ATTRIBUTE_PREFIX}.dataset.id", resolved_dataset_id)
        if resolved_dataset_version:
            span.set_attribute(
                f"{_ATTRIBUTE_PREFIX}.dataset.version", resolved_dataset_version
            )
        if resolved_format:
            span.set_attribute(f"{_ATTRIBUTE_PREFIX}.dataset.format", resolved_format)
        if table:
            span.set_attribute(f"{_ATTRIBUTE_PREFIX}.dataset.table", table)
        if path:
            span.set_attribute(f"{_ATTRIBUTE_PREFIX}.dataset.path", path)

        binding_payload = _binding_payload(resolved_binding)
        if binding_payload:
            span.set_attribute(
                f"{_ATTRIBUTE_PREFIX}.binding", _attribute_value(binding_payload)
            )

        flattened_context = _flatten_context(context)
        for key, value in flattened_context.items():
            span.set_attribute(f"{_ATTRIBUTE_PREFIX}.pipeline.{key}", value)

        validation_result = status or validation
        if validation_result is not None:
            span.set_attribute(
                f"{_ATTRIBUTE_PREFIX}.validation.status",
                validation_result.status or "unknown",
            )
            span.set_attribute(
                f"{_ATTRIBUTE_PREFIX}.validation.ok", bool(validation_result.ok)
            )
            if validation_result.reason:
                span.set_attribute(
                    f"{_ATTRIBUTE_PREFIX}.validation.reason",
                    validation_result.reason,
                )
            span.add_event(
                _VALIDATION_EVENT,
                _validation_event_payload(validation_result),
            )

        if resolved_expectations:
            span.add_event(
                _EXPECTATION_EVENT,
                {"plan": _attribute_value(_normalise_value(resolved_expectations))},
            )


__all__ = ["record_telemetry_span"]
