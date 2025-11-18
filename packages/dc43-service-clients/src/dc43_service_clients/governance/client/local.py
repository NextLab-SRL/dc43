"""Local governance client that delegates to in-process backends."""

from __future__ import annotations

from typing import Callable, Mapping, Optional, Sequence, TYPE_CHECKING

from open_data_contract_standard.model import OpenDataContractStandard  # type: ignore

from dc43_service_clients.data_quality import ObservationPayload, ValidationResult
from dc43_service_clients.governance.lineage import OpenDataLineageEvent
from dc43_service_clients.governance.models import (
    DatasetContractStatus,
    GovernanceCredentials,
    GovernanceReadContext,
    GovernanceWriteContext,
    PipelineContextSpec,
    QualityAssessment,
    QualityDraftContext,
    ResolvedReadPlan,
    ResolvedWritePlan,
)
from .interface import GovernanceServiceClient

if TYPE_CHECKING:  # pragma: no cover - imported for type checking only
    from dc43_service_backends.contracts import ContractServiceBackend, ContractStore
    from dc43_service_backends.data_products import DataProductServiceBackend
    from dc43_service_backends.data_quality import DataQualityServiceBackend
    from dc43_service_backends.governance.backend import (
        GovernanceServiceBackend,
        LocalGovernanceServiceBackend,
    )
else:  # pragma: no cover - satisfy type checkers without importing at runtime
    ContractServiceBackend = ContractStore = DataQualityServiceBackend = object  # type: ignore
    DataProductServiceBackend = object  # type: ignore
    GovernanceServiceBackend = LocalGovernanceServiceBackend = object  # type: ignore


class LocalGovernanceServiceClient(GovernanceServiceClient):
    """Delegate client calls to an in-process backend implementation."""

    def __init__(self, backend: "GovernanceServiceBackend") -> None:
        self._backend = backend

    def get_contract(
        self,
        *,
        contract_id: str,
        contract_version: str,
    ) -> OpenDataContractStandard:
        return self._backend.get_contract(
            contract_id=contract_id,
            contract_version=contract_version,
        )

    def latest_contract(
        self,
        *,
        contract_id: str,
    ) -> Optional[OpenDataContractStandard]:
        return self._backend.latest_contract(contract_id=contract_id)

    def list_contract_versions(self, *, contract_id: str) -> Sequence[str]:
        return self._backend.list_contract_versions(contract_id=contract_id)

    def describe_expectations(
        self,
        *,
        contract_id: str,
        contract_version: str,
    ) -> Sequence[Mapping[str, object]]:
        return self._backend.describe_expectations(
            contract_id=contract_id,
            contract_version=contract_version,
        )

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
        dq_status: ValidationResult | None = None,
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
    ) -> Optional[ValidationResult]:
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

    def get_metrics(
        self,
        *,
        dataset_id: str,
        dataset_version: Optional[str] = None,
        contract_id: Optional[str] = None,
        contract_version: Optional[str] = None,
    ) -> Sequence[Mapping[str, object]]:
        return self._backend.get_metrics(
            dataset_id=dataset_id,
            dataset_version=dataset_version,
            contract_id=contract_id,
            contract_version=contract_version,
        )

    def get_status_matrix(
        self,
        *,
        dataset_id: str,
        contract_ids: Sequence[str] | None = None,
        dataset_versions: Sequence[str] | None = None,
    ) -> Sequence[DatasetContractStatus]:
        records = self._backend.get_status_matrix(
            dataset_id=dataset_id,
            contract_ids=contract_ids,
            dataset_versions=dataset_versions,
        )
        return tuple(
            DatasetContractStatus(
                dataset_id=record["dataset_id"],
                dataset_version=record["dataset_version"],
                contract_id=record["contract_id"],
                contract_version=record["contract_version"],
                status=record.get("status"),
            )
            for record in records
        )

    def list_datasets(self) -> Sequence[str]:
        return self._backend.list_datasets()

    def get_pipeline_activity(
        self,
        *,
        dataset_id: str,
        dataset_version: Optional[str] = None,
        include_status: bool = False,
    ) -> Sequence[Mapping[str, object]]:
        return self._backend.get_pipeline_activity(
            dataset_id=dataset_id,
            dataset_version=dataset_version,
            include_status=include_status,
        )

    def get_dataset_records(
        self,
        *,
        dataset_id: str | None = None,
        dataset_version: str | None = None,
    ) -> Sequence[Mapping[str, object]]:
        records = self._backend.get_dataset_records(
            dataset_id=dataset_id,
            dataset_version=dataset_version,
        )
        return tuple(dict(record) for record in records)

    def resolve_read_context(
        self,
        *,
        context: GovernanceReadContext,
    ) -> ResolvedReadPlan:
        return self._backend.resolve_read_context(context=context)

    def resolve_write_context(
        self,
        *,
        context: GovernanceWriteContext,
    ) -> ResolvedWritePlan:
        return self._backend.resolve_write_context(context=context)

    def evaluate_read_plan(
        self,
        *,
        plan: ResolvedReadPlan,
        validation: ValidationResult | None,
        observations: Callable[[], ObservationPayload],
    ) -> QualityAssessment:
        return self._backend.evaluate_read_plan(
            plan=plan,
            validation=validation,
            observations=observations,
        )

    def evaluate_write_plan(
        self,
        *,
        plan: ResolvedWritePlan,
        validation: ValidationResult | None,
        observations: Callable[[], ObservationPayload],
    ) -> QualityAssessment:
        return self._backend.evaluate_write_plan(
            plan=plan,
            validation=validation,
            observations=observations,
        )

    def register_read_activity(
        self,
        *,
        plan: ResolvedReadPlan,
        assessment: QualityAssessment,
    ) -> None:
        self._backend.register_read_activity(plan=plan, assessment=assessment)

    def register_write_activity(
        self,
        *,
        plan: ResolvedWritePlan,
        assessment: QualityAssessment,
    ) -> None:
        self._backend.register_write_activity(plan=plan, assessment=assessment)

    def publish_lineage_event(
        self,
        *,
        event: OpenDataLineageEvent,
    ) -> None:
        self._backend.publish_lineage_event(event=event)


def build_local_governance_service(
    store: "ContractStore",
    *,
    contract_backend: "ContractServiceBackend | None" = None,
    dq_backend: "DataQualityServiceBackend | None" = None,
    data_product_backend: "DataProductServiceBackend | None" = None,
) -> LocalGovernanceServiceClient:
    """Construct a governance client wired against local backend stubs."""

    from dc43_service_backends.contracts import LocalContractServiceBackend
    from dc43_service_backends.data_quality import LocalDataQualityServiceBackend
    from dc43_service_backends.governance.backend import LocalGovernanceServiceBackend
    from dc43_service_backends.data_products import LocalDataProductServiceBackend

    contract_backend = contract_backend or LocalContractServiceBackend(store)
    dq_backend = dq_backend or LocalDataQualityServiceBackend()
    data_product_backend = data_product_backend or LocalDataProductServiceBackend()
    backend = LocalGovernanceServiceBackend(
        contract_client=contract_backend,
        dq_client=dq_backend,
        data_product_client=data_product_backend,
        draft_store=store,
    )
    return LocalGovernanceServiceClient(backend)


__all__ = ["LocalGovernanceServiceClient", "build_local_governance_service"]
