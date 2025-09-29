"""Client abstractions for governance orchestration."""

from __future__ import annotations

from typing import Mapping, Protocol

from open_data_contract_standard.model import OpenDataContractStandard  # type: ignore

from dc43_service_clients.data_quality import ObservationPayload, ValidationResult
from dc43_service_clients.governance.models import (
    PipelineContext,
    PipelineContextSpec,
    QualityAssessment,
    QualityDraftContext,
)


class GovernanceServiceClient(Protocol):
    """Protocol describing governance operations used by runtime integrations."""

    def draft_contract(
        self,
        *,
        dataset: PipelineContext,
        validation: ValidationResult,
        observation: ObservationPayload,
        contract: OpenDataContractStandard,
    ) -> QualityDraftContext:
        ...

    def submit_assessment(
        self,
        *,
        assessment: QualityAssessment,
    ) -> Mapping[str, object]:
        ...


__all__ = ["GovernanceServiceClient"]
