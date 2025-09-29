"""Serialisation helpers for data-quality service payloads."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from .models import ObservationPayload, ValidationResult


def encode_observation_payload(payload: ObservationPayload) -> dict[str, Any]:
    """Convert an :class:`ObservationPayload` into a JSON-safe mapping."""

    schema = None
    if payload.schema is not None:
        schema = {key: dict(value) for key, value in payload.schema.items()}
    return {
        "metrics": dict(payload.metrics),
        "schema": schema,
        "reused": bool(payload.reused),
    }


def decode_observation_payload(raw: Mapping[str, Any]) -> ObservationPayload:
    """Instantiate :class:`ObservationPayload` from JSON data."""

    metrics = raw.get("metrics") if isinstance(raw.get("metrics"), Mapping) else {}
    schema_raw = raw.get("schema")
    schema = None
    if isinstance(schema_raw, Mapping):
        schema = {key: dict(value) for key, value in schema_raw.items() if isinstance(value, Mapping)}
    reused = bool(raw.get("reused", False))
    return ObservationPayload(metrics=dict(metrics), schema=schema, reused=reused)


def encode_validation_result(result: ValidationResult | None) -> dict[str, Any] | None:
    """Convert a :class:`ValidationResult` into a JSON-safe mapping."""

    if result is None:
        return None
    return {
        "ok": bool(result.ok),
        "errors": list(result.errors),
        "warnings": list(result.warnings),
        "metrics": dict(result.metrics),
        "schema": {key: dict(value) for key, value in result.schema.items()},
        "status": result.status,
        "reason": result.reason,
        "details": dict(result.details),
    }


def decode_validation_result(raw: Mapping[str, Any] | None) -> ValidationResult | None:
    """Instantiate :class:`ValidationResult` from JSON data."""

    if raw is None:
        return None
    errors = raw.get("errors")
    warnings = raw.get("warnings")
    metrics = raw.get("metrics")
    schema = raw.get("schema")
    details = raw.get("details")
    reason = raw.get("reason")
    status = raw.get("status", "unknown")
    return ValidationResult(
        ok=bool(raw.get("ok", True)),
        errors=list(errors) if isinstance(errors, list) else None,
        warnings=list(warnings) if isinstance(warnings, list) else None,
        metrics=dict(metrics) if isinstance(metrics, Mapping) else None,
        schema={key: dict(value) for key, value in schema.items()} if isinstance(schema, Mapping) else None,
        status=str(status),
        reason=str(reason) if reason is not None else None,
        details=dict(details) if isinstance(details, Mapping) else details,
    )


__all__ = [
    "encode_observation_payload",
    "decode_observation_payload",
    "encode_validation_result",
    "decode_validation_result",
]
