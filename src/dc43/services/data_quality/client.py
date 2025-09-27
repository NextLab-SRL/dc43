"""Client abstractions for evaluating data-quality observations."""

from __future__ import annotations

from typing import Protocol

from open_data_contract_standard.model import OpenDataContractStandard  # type: ignore

from dc43.components.data_quality.engine import ValidationResult
from dc43.components.data_quality.manager import DataQualityManager

from .models import ObservationPayload


class DataQualityServiceClient(Protocol):
    """Protocol describing a data-quality service capable of evaluations."""

    def evaluate(
        self,
        *,
        contract: OpenDataContractStandard,
        payload: ObservationPayload,
    ) -> ValidationResult:
        """Return the validation outcome for the provided observations."""


class LocalDataQualityServiceClient(DataQualityServiceClient):
    """In-process implementation delegating to :class:`DataQualityManager`."""

    def __init__(self, manager: DataQualityManager | None = None) -> None:
        self._manager = manager or DataQualityManager()

    def evaluate(
        self,
        *,
        contract: OpenDataContractStandard,
        payload: ObservationPayload,
    ) -> ValidationResult:
        return self._manager.evaluate(contract, payload)


__all__ = [
    "DataQualityServiceClient",
    "LocalDataQualityServiceClient",
]
