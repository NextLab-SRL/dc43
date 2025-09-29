"""Local stub implementation of the data-quality backend contract."""

from __future__ import annotations

from open_data_contract_standard.model import OpenDataContractStandard  # type: ignore

from dc43_service_clients.data_quality import ObservationPayload, ValidationResult
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

    def describe_expectations(
        self, *, contract: OpenDataContractStandard
    ) -> list[dict[str, object]]:
        return self._manager.describe_expectations(contract)


__all__ = ["LocalDataQualityServiceBackend"]
