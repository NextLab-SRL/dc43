"""Data-quality engine primitives."""

from typing import Any, Literal, Mapping, Optional

from open_data_contract_standard.model import OpenDataContractStandard  # type: ignore

from .core import ExpectationSpec, ValidationResult, evaluate_contract, expectation_specs
from dc43.components.contract_drafter import draft_from_observations


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


def draft_from_validation_result(
    *,
    validation: ValidationResult,
    base_contract: OpenDataContractStandard,
    bump: str = "minor",
    dataset_id: Optional[str] = None,
    dataset_version: Optional[str] = None,
    data_format: Optional[str] = None,
    dq_feedback: Optional[Mapping[str, Any]] = None,
) -> OpenDataContractStandard:
    """Create a draft contract document from an engine validation result."""

    schema_payload: Mapping[str, Mapping[str, Any]] | None = validation.schema or None
    metrics_payload: Mapping[str, Any] | None = validation.metrics or None
    return draft_from_observations(
        schema=schema_payload or {},
        metrics=metrics_payload or None,
        base_contract=base_contract,
        bump=bump,
        dataset_id=dataset_id,
        dataset_version=dataset_version,
        data_format=data_format,
        dq_feedback=dict(dq_feedback) if dq_feedback else None,
    )


__all__ = [
    "ExpectationSpec",
    "ValidationResult",
    "evaluate_contract",
    "evaluate_observations",
    "draft_from_validation_result",
    "expectation_specs",
]
