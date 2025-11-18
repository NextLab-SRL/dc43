"""Service-side contract for governance orchestration."""

from __future__ import annotations

from typing import Callable, Mapping, Optional, Protocol, Sequence

from open_data_contract_standard.model import OpenDataContractStandard  # type: ignore

from dc43_service_clients.data_quality import ObservationPayload, ValidationResult
from dc43_service_clients.governance.lineage import OpenDataLineageEvent
from dc43_service_clients.governance.models import (
    GovernanceReadContext,
    GovernanceWriteContext,
    GovernanceCredentials,
    PipelineContextSpec,
    QualityAssessment,
    QualityDraftContext,
    ResolvedReadPlan,
    ResolvedWritePlan,
)


class GovernanceServiceBackend(Protocol):
    """Operations exposed by a governance service implementation."""

    def get_contract(
        self,
        *,
        contract_id: str,
        contract_version: str,
    ) -> OpenDataContractStandard:
        ...

    def latest_contract(
        self,
        *,
        contract_id: str,
    ) -> Optional[OpenDataContractStandard]:
        ...

    def list_contract_versions(self, *, contract_id: str) -> Sequence[str]:
        ...

    def describe_expectations(
        self,
        *,
        contract_id: str,
        contract_version: str,
    ) -> Sequence[Mapping[str, object]]:
        ...

    def configure_auth(
        self,
        credentials: GovernanceCredentials | Mapping[str, object] | str | None,
    ) -> None:
        ...

    def evaluate_dataset(
        self,
        *,
        contract_id: str,
        contract_version: str,
        dataset_id: str,
        dataset_version: str,
        validation: ValidationResult | None,
        observations: Callable[[], ObservationPayload],
        bump: str = "minor",
        context: QualityDraftContext | None = None,
        pipeline_context: PipelineContextSpec | None = None,
        operation: str = "read",
        draft_on_violation: bool = False,
    ) -> QualityAssessment:
        ...

    def review_validation_outcome(
        self,
        *,
        validation: ValidationResult,
        base_contract: OpenDataContractStandard,
        bump: str = "minor",
        dataset_id: str | None = None,
        dataset_version: str | None = None,
        data_format: str | None = None,
        dq_status: ValidationResult | None = None,
        dq_feedback: Mapping[str, object] | None = None,
        context: QualityDraftContext | None = None,
        pipeline_context: PipelineContextSpec | None = None,
        draft_requested: bool = False,
        operation: str | None = None,
    ) -> Optional[OpenDataContractStandard]:
        ...

    def propose_draft(
        self,
        *,
        validation: ValidationResult,
        base_contract: OpenDataContractStandard,
        bump: str = "minor",
        context: QualityDraftContext | None = None,
        pipeline_context: PipelineContextSpec | None = None,
    ) -> OpenDataContractStandard:
        ...

    def get_status(
        self,
        *,
        contract_id: str,
        contract_version: str,
        dataset_id: str,
        dataset_version: str,
    ) -> Optional[ValidationResult]:
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

    def get_metrics(
        self,
        *,
        dataset_id: str,
        dataset_version: Optional[str] = None,
        contract_id: Optional[str] = None,
        contract_version: Optional[str] = None,
    ) -> Sequence[Mapping[str, object]]:
        ...

    def get_status_matrix(
        self,
        *,
        dataset_id: str,
        contract_ids: Sequence[str] | None = None,
        dataset_versions: Sequence[str] | None = None,
    ) -> Sequence[Mapping[str, object]]:
        ...

    def list_datasets(self) -> Sequence[str]:
        ...

    def get_pipeline_activity(
        self,
        *,
        dataset_id: str,
        dataset_version: Optional[str] = None,
        include_status: bool = False,
    ) -> Sequence[Mapping[str, object]]:
        ...

    def get_dataset_records(
        self,
        *,
        dataset_id: str | None = None,
        dataset_version: str | None = None,
    ) -> Sequence[Mapping[str, object]]:
        ...

    def resolve_read_context(
        self,
        *,
        context: GovernanceReadContext,
    ) -> ResolvedReadPlan:
        ...

    def resolve_write_context(
        self,
        *,
        context: GovernanceWriteContext,
    ) -> ResolvedWritePlan:
        ...

    def evaluate_read_plan(
        self,
        *,
        plan: ResolvedReadPlan,
        validation: ValidationResult | None,
        observations: Callable[[], ObservationPayload],
    ) -> QualityAssessment:
        ...

    def evaluate_write_plan(
        self,
        *,
        plan: ResolvedWritePlan,
        validation: ValidationResult | None,
        observations: Callable[[], ObservationPayload],
    ) -> QualityAssessment:
        ...

    def register_read_activity(
        self,
        *,
        plan: ResolvedReadPlan,
        assessment: QualityAssessment,
    ) -> None:
        ...

    def register_write_activity(
        self,
        *,
        plan: ResolvedWritePlan,
        assessment: QualityAssessment,
    ) -> None:
        ...

    def publish_lineage_event(
        self,
        *,
        event: OpenDataLineageEvent,
    ) -> None:
        ...


__all__ = ["GovernanceServiceBackend"]
