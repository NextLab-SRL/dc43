"""In-process implementation of the data-quality client contract."""

from __future__ import annotations

from open_data_contract_standard.model import OpenDataContractStandard  # type: ignore

from dc43.services.data_quality.models import ObservationPayload, ValidationResult

from ..backend import DataQualityServiceBackend, LocalDataQualityServiceBackend
from .interface import DataQualityServiceClient


class LocalDataQualityServiceClient(DataQualityServiceClient):
    """Invoke a backend implementation without requiring HTTP plumbing."""

    def __init__(self, backend: DataQualityServiceBackend | None = None) -> None:
        self._backend = backend or LocalDataQualityServiceBackend()

    def evaluate(
        self,
        *,
        contract: OpenDataContractStandard,
        payload: ObservationPayload,
    ) -> ValidationResult:
        return self._backend.evaluate(contract=contract, payload=payload)

    def describe_expectations(
        self, *, contract: OpenDataContractStandard
    ) -> list[dict[str, object]]:
        descriptors = self._backend.describe_expectations(contract=contract)
        return list(descriptors)


__all__ = ["LocalDataQualityServiceClient"]
