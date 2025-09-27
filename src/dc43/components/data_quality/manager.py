"""Lightweight data-quality manager backed by the validation engine."""

from __future__ import annotations

from open_data_contract_standard.model import OpenDataContractStandard  # type: ignore

from dc43.components.contract_validation import (
    ValidationResult,
    evaluate_observations,
)
from dc43.lib.data_quality import ObservationPayload


class DataQualityManager:
    """Evaluate observation payloads using the runtime-agnostic engine."""

    def __init__(
        self,
        *,
        strict_types: bool = True,
        allow_extra_columns: bool = True,
        expectation_severity: str = "error",
    ) -> None:
        self._strict_types = strict_types
        self._allow_extra_columns = allow_extra_columns
        self._expectation_severity = expectation_severity

    def evaluate(
        self,
        contract: OpenDataContractStandard,
        payload: ObservationPayload,
    ) -> ValidationResult:
        """Return the validation outcome for the provided observations."""

        return evaluate_observations(
            contract,
            schema=payload.schema,
            metrics=payload.metrics,
            strict_types=self._strict_types,
            allow_extra_columns=self._allow_extra_columns,
            expectation_severity=self._expectation_severity,  # type: ignore[arg-type]
        )


__all__ = ["DataQualityManager", "ObservationPayload", "ValidationResult"]
