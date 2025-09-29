"""Interfaces for running data-quality service backends."""

from __future__ import annotations

from typing import Mapping, Protocol, Sequence

from open_data_contract_standard.model import OpenDataContractStandard  # type: ignore

from dc43_service_clients.data_quality import ObservationPayload, ValidationResult


class DataQualityServiceBackend(Protocol):
    """Service-side contract for evaluating data-quality observations."""

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


__all__ = ["DataQualityServiceBackend"]
