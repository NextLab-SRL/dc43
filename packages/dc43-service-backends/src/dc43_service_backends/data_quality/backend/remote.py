"""HTTP delegate implementing the data-quality backend interface."""

from __future__ import annotations

from typing import Mapping, Sequence

from open_data_contract_standard.model import OpenDataContractStandard  # type: ignore

from dc43_service_clients.data_quality import ObservationPayload, ValidationResult
from dc43_service_clients.data_quality.client.interface import DataQualityServiceClient

from .interface import DataQualityServiceBackend


class RemoteDataQualityServiceBackend(DataQualityServiceBackend):
    """Forward data-quality evaluations to a remote HTTP endpoint."""

    def __init__(self, client: DataQualityServiceClient) -> None:
        self._client = client

    def evaluate(
        self,
        *,
        contract: OpenDataContractStandard,
        payload: ObservationPayload,
    ) -> ValidationResult:
        return self._client.evaluate(contract=contract, payload=payload)

    def describe_expectations(
        self,
        *,
        contract: OpenDataContractStandard,
    ) -> Sequence[Mapping[str, object]]:
        return self._client.describe_expectations(contract=contract)

    def close(self) -> None:
        """Close the underlying client when it exposes a ``close`` hook."""

        try:
            closer = self._client.close
        except AttributeError:
            return
        closer()


__all__ = ["RemoteDataQualityServiceBackend"]

