"""Local stub implementation of the data-quality backend contract."""

from __future__ import annotations

from open_data_contract_standard.model import OpenDataContractStandard  # type: ignore

from dc43.services.data_quality.models import ObservationPayload, ValidationResult
from .manager import DataQualityManager

from .interface import DataQualityServiceBackend


class LocalDataQualityServiceBackend(DataQualityServiceBackend):
    """Adapter delegating to :class:`DataQualityManager` for evaluations."""

    def __init__(self, manager: DataQualityManager | None = None) -> None:
        self._manager = manager or DataQualityManager()

    def evaluate(
        self,
        *,
        contract: OpenDataContractStandard,
        payload: ObservationPayload,
    ) -> ValidationResult:
        return self._manager.evaluate(contract, payload)


__all__ = ["LocalDataQualityServiceBackend"]
