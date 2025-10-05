"""Client abstractions for governance orchestration."""

from __future__ import annotations

from typing import Mapping, Optional, Protocol

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

    def link_dataset_contract(
        self,
        *,
        dataset_id: str,
        dataset_version: str,
        contract_id: str,
        contract_version: str,
    ) -> None:
        ...

    def get_linked_contract_version(
        self,
        *,
        dataset_id: str,
        dataset_version: Optional[str] = None,
    ) -> Optional[str]:
        ...


__all__ = ["GovernanceServiceClient"]
