"""Helpers for normalising metrics stored with validation results."""

from __future__ import annotations

from typing import Any, Mapping

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


__all__ = ["extract_metrics"]

