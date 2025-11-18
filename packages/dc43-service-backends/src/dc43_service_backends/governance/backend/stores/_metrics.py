"""Helpers for normalising metrics stored with validation results."""

from __future__ import annotations

import json
from numbers import Number
from typing import Any, Mapping, Tuple

from dc43_service_clients.data_quality import ValidationResult


def extract_metrics(status: ValidationResult | None) -> dict[str, Any]:
    """Return a serialisable metrics mapping for ``status``.

    Some validation providers only attach metric observations inside the
    :pyattr:`ValidationResult.details` payload instead of the ``metrics``
    attribute. The governance stores expect explicit values to populate the
    ``dq_metrics`` tables, so we merge both sources to avoid dropping data.
    """

    if status is None:
        return {}

    metrics: dict[str, Any] = {}
    details = status.details
    detail_metrics = details.get("metrics") if isinstance(details, Mapping) else None
    if isinstance(detail_metrics, Mapping):
        metrics.update(detail_metrics)
    metrics.update(status.metrics or {})
    return metrics


def normalise_metric_value(value: Any) -> Tuple[str | None, float | None]:
    """Return storage-friendly metric payloads plus numeric hints.

    SQL, Delta, and filesystem stores persist metric observations so the UI can
    display historical charts. The persistence layer stores values as text, but
    callers often provide numbers (or numeric strings) that should populate the
    ``metric_numeric_value`` field to simplify filtering. This helper mirrors the
    UI's coercion logic by returning the serialised value alongside a
    floating-point representation whenever the input resembles a number.
    """

    if value is None:
        return None, None
    if isinstance(value, Number):
        return str(value), float(value)
    if isinstance(value, str):
        stripped = value.strip()
        if stripped:
            try:
                numeric = float(stripped)
            except ValueError:
                numeric = None
        else:
            numeric = None
        return value, numeric
    try:
        serialised = json.dumps(value)
    except TypeError:
        serialised = str(value)
    return serialised, None


__all__ = ["extract_metrics", "normalise_metric_value"]

