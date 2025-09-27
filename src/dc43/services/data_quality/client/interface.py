"""Client abstractions for evaluating data-quality observations."""

from __future__ import annotations

from typing import Protocol

from open_data_contract_standard.model import OpenDataContractStandard  # type: ignore

from dc43.services.data_quality.backend.engine import ValidationResult
from dc43.services.data_quality.models import ObservationPayload


class DataQualityServiceClient(Protocol):
    """Protocol describing a data-quality service capable of evaluations."""

    def evaluate(
        self,
        *,
        contract: OpenDataContractStandard,
        payload: ObservationPayload,
    ) -> ValidationResult:
        """Return the validation outcome for the provided observations."""


__all__ = ["DataQualityServiceClient"]
