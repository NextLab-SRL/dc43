"""Interfaces for running data-quality service backends."""

from __future__ import annotations

from typing import Protocol

from open_data_contract_standard.model import OpenDataContractStandard  # type: ignore

from dc43.components.contract_validation import ValidationResult
from dc43.lib.data_quality import ObservationPayload


class DataQualityServiceBackend(Protocol):
    """Service-side contract for evaluating data-quality observations."""

    def evaluate(
        self,
        *,
        contract: OpenDataContractStandard,
        payload: ObservationPayload,
    ) -> ValidationResult:
        """Return the validation outcome for the provided observations."""


__all__ = ["DataQualityServiceBackend"]
