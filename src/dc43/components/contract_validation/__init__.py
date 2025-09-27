"""Compatibility exports for the contract validation engine.

The runtime-agnostic evaluation logic now resides under
:mod:`dc43.services.data_quality.backend`.  This module re-exports the public
API so library consumers continue working while migrations happen.
"""

from typing import Any, Literal, Mapping

from open_data_contract_standard.model import OpenDataContractStandard  # type: ignore

from dc43.services.data_quality.backend.engine import (
    ExpectationSpec,
    ValidationResult,
    evaluate_contract,
    expectation_specs,
)


def evaluate_observations(
    contract: OpenDataContractStandard,
    *,
    schema: Mapping[str, Mapping[str, Any]] | None,
    metrics: Mapping[str, Any] | None,
    strict_types: bool = True,
    allow_extra_columns: bool = True,
    expectation_severity: Literal["error", "warning", "ignore"] = "error",
):
    """Evaluate cached observations using the runtime-agnostic engine."""

    return evaluate_contract(
        contract,
        schema=schema,
        metrics=metrics,
        strict_types=strict_types,
        allow_extra_columns=allow_extra_columns,
        expectation_severity=expectation_severity,
    )


__all__ = [
    "ExpectationSpec",
    "ValidationResult",
    "evaluate_contract",
    "evaluate_observations",
    "expectation_specs",
]
