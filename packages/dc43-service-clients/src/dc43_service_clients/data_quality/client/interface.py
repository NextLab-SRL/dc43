"""Client abstractions for evaluating data-quality observations."""

from __future__ import annotations

from typing import Mapping, Protocol, Sequence

from open_data_contract_standard.model import OpenDataContractStandard  # type: ignore

from dc43_service_clients.data_quality import ObservationPayload, ValidationResult


class DataQualityServiceClient(Protocol):
    """Protocol describing a data-quality service capable of evaluations."""

    def evaluate(
        self,
        *,
        contract: OpenDataContractStandard,
        payload: ObservationPayload,
    ) -> ValidationResult:
        """Return the validation outcome for the provided observations."""

    def describe_expectations(
        self, *, contract: OpenDataContractStandard
    ) -> Sequence[Mapping[str, object]]:
        """Return serialisable descriptors for contract expectations."""


__all__ = ["DataQualityServiceClient"]
