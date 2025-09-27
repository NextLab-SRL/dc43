"""Local orchestration service coordinating contract and quality clients."""

from __future__ import annotations

from typing import Any, Callable, Dict, Mapping, Optional, Sequence

from open_data_contract_standard.model import OpenDataContractStandard  # type: ignore

from dc43.components.contract_drafter import draft_from_validation_result
from dc43.components.contract_store.interface import ContractStore
from dc43.components.data_quality.engine import ValidationResult
from dc43.components.data_quality.governance import DQStatus
from dc43.odcs import contract_identity
from dc43.services.contracts import ContractServiceClient
from dc43.services.contracts.local import LocalContractServiceClient
from dc43.services.data_quality import DataQualityServiceClient, LocalDataQualityServiceClient
from dc43.lib.data_quality import ObservationPayload

from .client import GovernanceServiceClient
from .models import (
    GovernanceCredentials,
    PipelineContextSpec,
    QualityAssessment,
    QualityDraftContext,
    build_quality_context,
    derive_feedback,
    merge_pipeline_context,
)


class LocalGovernanceService(GovernanceServiceClient):
    """In-process orchestration across contract and data-quality services."""

    def __init__(
        self,
        *,
        contract_client: ContractServiceClient,
        dq_client: DataQualityServiceClient,
        draft_store: ContractStore | None = None,
    ) -> None:
        self._contract_client = contract_client
        self._dq_client = dq_client
        self._draft_store = draft_store
        self._credentials: Optional[GovernanceCredentials] = None
        self._status_cache: Dict[tuple[str, str, str, str], DQStatus] = {}
        self._activity_log: Dict[tuple[str, Optional[str]], list[Mapping[str, Any]]] = {}

    # ------------------------------------------------------------------
    # Authentication lifecycle
    # ------------------------------------------------------------------
    def configure_auth(
        self,
        credentials: GovernanceCredentials | Mapping[str, object] | str | None,
    ) -> None:
        if credentials is None:
            self._credentials = None
            return
        if isinstance(credentials, GovernanceCredentials):
            self._credentials = credentials
            return
        if isinstance(credentials, str):
            self._credentials = GovernanceCredentials(token=credentials)
            return
        token = str(credentials.get("token")) if "token" in credentials else None
        headers = credentials.get("headers") if isinstance(credentials.get("headers"), Mapping) else None
        extra = {
            key: value
            for key, value in credentials.items()
            if key not in {"token", "headers"}
        }
        self._credentials = GovernanceCredentials(
            token=token,
            headers=headers,  # type: ignore[arg-type]
            extra=extra or None,
        )

    @property
    def credentials(self) -> Optional[GovernanceCredentials]:
        return self._credentials

    # ------------------------------------------------------------------
    # Governance orchestration
    # ------------------------------------------------------------------
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
        contract = self._contract_client.get(contract_id, contract_version)

        payload = observations()
        validation = validation or self._dq_client.evaluate(
            contract=contract,
            payload=payload,
        )
        status = self._status_from_validation(validation)

        cache_key = (contract_id, contract_version, dataset_id, dataset_version)
        self._status_cache[cache_key] = status

        effective_pipeline = merge_pipeline_context(
            context.pipeline_context if context else None,
            pipeline_context,
            {"io": operation},
        )

        draft: Optional[OpenDataContractStandard] = None
        if draft_on_violation and status and status.status in {"warn", "block"}:
            draft = self.review_validation_outcome(
                validation=validation,
                base_contract=contract,
                bump=bump,
                dataset_id=context.dataset_id if context else dataset_id,
                dataset_version=context.dataset_version if context else dataset_version,
                data_format=context.data_format if context else None,
                dq_status=status,
                dq_feedback=context.dq_feedback if context else None,
                context=context,
                pipeline_context=effective_pipeline,
                draft_requested=True,
                operation=operation,
            )
            if draft is not None and status is not None:
                details = dict(status.details or {})
                details.setdefault("draft_contract_version", draft.version)
                status.details = details

        self._record_pipeline_activity(
            contract=contract,
            dataset_id=dataset_id,
            dataset_version=dataset_version,
            operation=operation,
            pipeline_context=effective_pipeline,
            status=status,
            observations_reused=payload.reused,
        )

        return QualityAssessment(status=status, draft=draft, observations_reused=payload.reused)

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
        if not draft_requested:
            return None

        effective_context = build_quality_context(
            context,
            dataset_id=dataset_id,
            dataset_version=dataset_version,
            data_format=data_format,
            dq_feedback=derive_feedback(dq_status, dq_feedback),
            pipeline_context=merge_pipeline_context(
                context.pipeline_context if context else None,
                pipeline_context,
                {"io": operation} if operation else None,
            ),
        )

        draft = self.propose_draft(
            validation=validation,
            base_contract=base_contract,
            bump=bump,
            context=effective_context,
            pipeline_context=effective_context.pipeline_context,
        )

        if draft is not None and self._draft_store is not None:
            self._draft_store.put(draft)

        return draft

    def propose_draft(
        self,
        *,
        validation: ValidationResult,
        base_contract: OpenDataContractStandard,
        bump: str = "minor",
        context: QualityDraftContext | None = None,
        pipeline_context: PipelineContextSpec | None = None,
    ) -> OpenDataContractStandard:
        effective_context = build_quality_context(
            context,
            dataset_id=context.dataset_id if context else None,
            dataset_version=context.dataset_version if context else None,
            data_format=context.data_format if context else None,
            dq_feedback=context.dq_feedback if context else None,
            pipeline_context=pipeline_context,
        )

        draft = draft_from_validation_result(
            validation=validation,
            base_contract=base_contract,
            bump=bump,
            dataset_id=effective_context.dataset_id,
            dataset_version=effective_context.dataset_version,
            data_format=effective_context.data_format,
            dq_feedback=effective_context.dq_feedback,
            draft_context=effective_context.draft_context,
        )
        if draft is not None and self._draft_store is not None:
            self._draft_store.put(draft)
        return draft

    def get_status(
        self,
        *,
        contract_id: str,
        contract_version: str,
        dataset_id: str,
        dataset_version: str,
    ) -> Optional[DQStatus]:
        return self._status_cache.get((contract_id, contract_version, dataset_id, dataset_version))

    def link_dataset_contract(
        self,
        *,
        dataset_id: str,
        dataset_version: str,
        contract_id: str,
        contract_version: str,
    ) -> None:
        linker = getattr(self._contract_client, "link_dataset_contract", None)
        if callable(linker):
            linker(
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
        resolver = getattr(self._contract_client, "get_linked_contract_version", None)
        if callable(resolver):
            return resolver(dataset_id=dataset_id, dataset_version=dataset_version)
        return None

    def get_pipeline_activity(
        self,
        *,
        dataset_id: str,
        dataset_version: Optional[str] = None,
    ) -> Sequence[Mapping[str, Any]]:
        return list(self._activity_log.get((dataset_id, dataset_version), []))

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------
    def _status_from_validation(self, validation: ValidationResult) -> DQStatus:
        if validation.ok:
            return DQStatus(status="ok", details={"violations": 0})
        if validation.errors:
            return DQStatus(
                status="block",
                reason=validation.errors[0],
                details={
                    "errors": list(validation.errors),
                    "warnings": list(validation.warnings),
                    "violations": len(validation.errors),
                },
            )
        return DQStatus(
            status="warn",
            reason=validation.warnings[0] if validation.warnings else None,
            details={
                "warnings": list(validation.warnings),
                "violations": len(validation.warnings),
            },
        )

    def _record_pipeline_activity(
        self,
        *,
        contract: OpenDataContractStandard,
        dataset_id: str,
        dataset_version: str,
        operation: str,
        pipeline_context: Optional[Mapping[str, Any]],
        status: Optional[DQStatus],
        observations_reused: bool,
    ) -> None:
        cid, cver = contract_identity(contract)
        entry: Dict[str, Any] = {
            "operation": operation,
            "contract_id": cid,
            "contract_version": cver,
            "pipeline_context": dict(pipeline_context or {}),
            "observations_reused": observations_reused,
        }
        if status:
            entry["dq_status"] = status.status
            if status.reason:
                entry["dq_reason"] = status.reason
            if status.details:
                entry["dq_details"] = status.details
        key = (dataset_id, dataset_version)
        self._activity_log.setdefault(key, []).append(entry)


def build_local_governance_service(
    store: ContractStore,
) -> LocalGovernanceService:
    """Construct a governance service wired against local stubs."""

    contract_client = LocalContractServiceClient(store)
    dq_client = LocalDataQualityServiceClient()
    return LocalGovernanceService(
        contract_client=contract_client,
        dq_client=dq_client,
        draft_store=store,
    )


__all__ = ["LocalGovernanceService", "build_local_governance_service"]
