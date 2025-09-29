"""Lightweight data-quality manager backed by the validation engine."""

from __future__ import annotations

from open_data_contract_standard.model import OpenDataContractStandard  # type: ignore

from dc43_service_backends.data_quality.backend.engine import (
    ValidationResult,
    evaluate_contract,
)
from dc43_service_backends.data_quality.backend.predicates import (
    expectation_plan,
    expectation_predicates_from_plan,
)
from dc43_service_clients.data_quality import ObservationPayload


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

        result = evaluate_contract(
            contract,
            schema=payload.schema,
            metrics=payload.metrics,
            strict_types=self._strict_types,
            allow_extra_columns=self._allow_extra_columns,
            expectation_severity=self._expectation_severity,  # type: ignore[arg-type]
        )
        plan = expectation_plan(contract)
        if plan:
            payloads = {"expectation_plan": plan}
            predicates = expectation_predicates_from_plan(plan)
            if predicates:
                payloads["expectation_predicates"] = predicates
            result.merge_details(payloads)
        return result

    def describe_expectations(
        self, contract: OpenDataContractStandard
    ) -> list[dict[str, object]]:
        """Return serialisable expectation descriptors for ``contract``."""

        return expectation_plan(contract)


__all__ = ["DataQualityManager", "ObservationPayload", "ValidationResult"]
