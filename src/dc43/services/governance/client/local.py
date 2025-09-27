"""Local client for governance orchestration services."""

from __future__ import annotations

from typing import Callable, Mapping, Optional, Sequence

from open_data_contract_standard.model import OpenDataContractStandard  # type: ignore

from dc43.components.contract_store.interface import ContractStore
from dc43.components.contract_validation import ValidationResult
from dc43.components.data_quality.governance import DQStatus
from dc43.lib.data_quality import ObservationPayload
from dc43.services.contracts.backend import (
    ContractServiceBackend,
    LocalContractServiceBackend,
)
from dc43.services.data_quality.backend import (
    DataQualityServiceBackend,
    LocalDataQualityServiceBackend,
)

from ..backend import GovernanceServiceBackend, LocalGovernanceServiceBackend
from ..models import (
    GovernanceCredentials,
    PipelineContextSpec,
    QualityAssessment,
    QualityDraftContext,
)
from .interface import GovernanceServiceClient


class LocalGovernanceServiceClient(GovernanceServiceClient):
    """Delegate client calls to an in-process backend implementation."""

    def __init__(self, backend: GovernanceServiceBackend) -> None:
        self._backend = backend

    def configure_auth(
        self,
        credentials: GovernanceCredentials | Mapping[str, object] | str | None,
    ) -> None:
        self._backend.configure_auth(credentials)

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
        return self._backend.evaluate_dataset(
            contract_id=contract_id,
            contract_version=contract_version,
            dataset_id=dataset_id,
            dataset_version=dataset_version,
            validation=validation,
            observations=observations,
            bump=bump,
            context=context,
            pipeline_context=pipeline_context,
            operation=operation,
            draft_on_violation=draft_on_violation,
        )

    def review_validation_outcome(
        self,
        *,
        validation: ValidationResult,
        base_contract: OpenDataContractStandard,
        bump: str = "minor",
        dataset_id: str | None = None,
        dataset_version: str | None = None,
        data_format: str | None = None,
        dq_status: DQStatus | None = None,
        dq_feedback: Mapping[str, object] | None = None,
        context: QualityDraftContext | None = None,
        pipeline_context: PipelineContextSpec | None = None,
        draft_requested: bool = False,
        operation: str | None = None,
    ) -> Optional[OpenDataContractStandard]:
        return self._backend.review_validation_outcome(
            validation=validation,
            base_contract=base_contract,
            bump=bump,
            dataset_id=dataset_id,
            dataset_version=dataset_version,
            data_format=data_format,
            dq_status=dq_status,
            dq_feedback=dq_feedback,
            context=context,
            pipeline_context=pipeline_context,
            draft_requested=draft_requested,
            operation=operation,
        )

    def propose_draft(
        self,
        *,
        validation: ValidationResult,
        base_contract: OpenDataContractStandard,
        bump: str = "minor",
        context: QualityDraftContext | None = None,
        pipeline_context: PipelineContextSpec | None = None,
    ) -> OpenDataContractStandard:
        return self._backend.propose_draft(
            validation=validation,
            base_contract=base_contract,
            bump=bump,
            context=context,
            pipeline_context=pipeline_context,
        )

    def get_status(
        self,
        *,
        contract_id: str,
        contract_version: str,
        dataset_id: str,
        dataset_version: str,
    ) -> Optional[DQStatus]:
        return self._backend.get_status(
            contract_id=contract_id,
            contract_version=contract_version,
            dataset_id=dataset_id,
            dataset_version=dataset_version,
        )

    def link_dataset_contract(
        self,
        *,
        dataset_id: str,
        dataset_version: str,
        contract_id: str,
        contract_version: str,
    ) -> None:
        self._backend.link_dataset_contract(
            dataset_id=dataset_id,
            dataset_version=dataset_version,
            contract_id=contract_id,
            contract_version=contract_version,
        )

    def get_linked_contract_version(
        self,
        *,
        dataset_id: str,
        dataset_version: Optional[str] = None,
    ) -> Optional[str]:
        return self._backend.get_linked_contract_version(
            dataset_id=dataset_id,
            dataset_version=dataset_version,
        )

    def get_pipeline_activity(
        self,
        *,
        dataset_id: str,
        dataset_version: Optional[str] = None,
    ) -> Sequence[Mapping[str, object]]:
        return self._backend.get_pipeline_activity(
            dataset_id=dataset_id,
            dataset_version=dataset_version,
        )


def build_local_governance_service(
    store: ContractStore,
    *,
    contract_backend: ContractServiceBackend | None = None,
    dq_backend: DataQualityServiceBackend | None = None,
) -> LocalGovernanceServiceClient:
    """Construct a governance client wired against local backend stubs."""

    contract_backend = contract_backend or LocalContractServiceBackend(store)
    dq_backend = dq_backend or LocalDataQualityServiceBackend()
    backend = LocalGovernanceServiceBackend(
        contract_client=contract_backend,
        dq_client=dq_backend,
        draft_store=store,
    )
    return LocalGovernanceServiceClient(backend)


__all__ = ["LocalGovernanceServiceClient", "build_local_governance_service"]
