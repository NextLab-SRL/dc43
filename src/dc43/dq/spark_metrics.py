"""Compatibility shim for Spark data-quality helpers."""

from dc43.components.data_quality.engine import (
    attach_failed_expectations,
    compute_metrics,
    expectations_from_contract,
)

__all__ = [
    "attach_failed_expectations",
    "compute_metrics",
    "expectations_from_contract",
]
