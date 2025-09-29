"""In-process implementation of the data-quality client contract."""

from __future__ import annotations

from typing import TYPE_CHECKING

from open_data_contract_standard.model import OpenDataContractStandard  # type: ignore

from dc43_service_clients.data_quality import ObservationPayload, ValidationResult
from .interface import DataQualityServiceClient

if TYPE_CHECKING:  # pragma: no cover - imported for type checking only
    from dc43_service_backends.data_quality import DataQualityServiceBackend
else:  # pragma: no cover - satisfy type checkers without importing at runtime
    DataQualityServiceBackend = object  # type: ignore


class LocalDataQualityServiceClient(DataQualityServiceClient):
    """Invoke a backend implementation without requiring HTTP plumbing."""

    def __init__(self, backend: "DataQualityServiceBackend | None" = None) -> None:
        if backend is None:
            from dc43_service_backends.data_quality import LocalDataQualityServiceBackend

            backend = LocalDataQualityServiceBackend()
        self._backend = backend

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
