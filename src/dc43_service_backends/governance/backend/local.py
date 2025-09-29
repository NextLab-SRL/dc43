"""In-process governance backend coordinating contract and quality services."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Callable, Dict, Mapping, Optional, Sequence

from open_data_contract_standard.model import OpenDataContractStandard  # type: ignore

from dc43.odcs import contract_identity
from dc43_service_backends.contracts import ContractServiceBackend, ContractStore
from dc43_service_backends.contracts.drafting import draft_from_validation_result
from dc43_service_backends.data_quality import DataQualityServiceBackend
from dc43_service_clients.contracts import ContractServiceClient
from dc43_service_clients.data_quality import (
    DataQualityServiceClient,
    ObservationPayload,
    ValidationResult,
)

from .interface import GovernanceServiceBackend
from dc43_service_clients.governance.models import (
    GovernanceCredentials,
    PipelineContextSpec,
    QualityAssessment,
    QualityDraftContext,
    build_quality_context,
    derive_feedback,
    merge_pipeline_context,
)


class LocalGovernanceServiceBackend(GovernanceServiceBackend):
    """In-process orchestration across contract and data-quality services."""

    def __init__(
        self,
        *,
        contract_client: ContractServiceBackend | ContractServiceClient,
        dq_client: DataQualityServiceBackend | DataQualityServiceClient,
        draft_store: ContractStore | None = None,
    ) -> None:
        self._contract_client = contract_client
        self._dq_client = dq_client
        self._draft_store = draft_store
        self._credentials: Optional[GovernanceCredentials] = None
        self._status_cache: Dict[tuple[str, str, str, str], ValidationResult] = {}
        self._activity_log: Dict[str, Dict[Optional[str], Dict[str, Any]]] = {}
        self._dataset_links: Dict[tuple[str, Optional[str]], str] = {}

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
        status = self._status_from_validation(validation, operation=operation)

        if status is not None:
            details = dict(status.details)
            if payload.metrics:
                if not details.get("metrics"):
                    details["metrics"] = payload.metrics
                status.metrics = dict(payload.metrics)
            if payload.schema:
                if not details.get("schema"):
                    details["schema"] = payload.schema
                status.schema = dict(payload.schema)
            status.details = details

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
                details = dict(status.details)
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
        dq_status: ValidationResult | None = None,
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
    ) -> Optional[ValidationResult]:
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
        link_value = f"{contract_id}:{contract_version}"
        self._dataset_links[(dataset_id, dataset_version)] = link_value
        if dataset_version is not None:
            self._dataset_links.setdefault((dataset_id, None), link_value)

    def get_linked_contract_version(
        self,
        *,
        dataset_id: str,
        dataset_version: Optional[str] = None,
    ) -> Optional[str]:
        resolver = getattr(self._contract_client, "get_linked_contract_version", None)
        if callable(resolver):
            resolved = resolver(dataset_id=dataset_id, dataset_version=dataset_version)
            if resolved is not None:
                return resolved
        if (dataset_id, dataset_version) in self._dataset_links:
            return self._dataset_links[(dataset_id, dataset_version)]
        if dataset_version is not None and (dataset_id, None) in self._dataset_links:
            return self._dataset_links[(dataset_id, None)]
        for (linked_id, linked_version), value in self._dataset_links.items():
            if linked_id == dataset_id:
                if dataset_version is None or linked_version == dataset_version:
                    return value
        return None

    def get_pipeline_activity(
        self,
        *,
        dataset_id: str,
        dataset_version: Optional[str] = None,
    ) -> Sequence[Mapping[str, Any]]:
        dataset_entries = self._activity_log.get(dataset_id)
        if not dataset_entries:
            return []

        def _normalise(record: Mapping[str, Any]) -> Dict[str, Any]:
            entry = {
                "dataset_id": dataset_id,
                "dataset_version": record.get("dataset_version"),
                "contract_id": record.get("contract_id"),
                "contract_version": record.get("contract_version"),
                "events": [],
            }
            events = record.get("events")
            if isinstance(events, list):
                entry["events"] = [
                    dict(event)
                    for event in events
                    if isinstance(event, Mapping)
                ]
            return entry

        if dataset_version is not None:
            record = dataset_entries.get(dataset_version)
            if isinstance(record, Mapping):
                return [_normalise(record)]
            for candidate in dataset_entries.values():
                if (
                    isinstance(candidate, Mapping)
                    and candidate.get("dataset_version") == dataset_version
                ):
                    return [_normalise(candidate)]
            return []

        entries: list[Dict[str, Any]] = []
        for record in dataset_entries.values():
            if isinstance(record, Mapping):
                entries.append(_normalise(record))

        def _sort_key(item: Mapping[str, Any]) -> tuple[int, str]:
            events = item.get("events")
            recorded_at = ""
            if isinstance(events, list) and events:
                last = events[-1]
                if isinstance(last, Mapping):
                    recorded_at = str(last.get("recorded_at", ""))
            version_value = str(item.get("dataset_version", ""))
            return (0 if recorded_at else 1, recorded_at or version_value)

        entries.sort(key=_sort_key)
        return entries

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------
    def _status_from_validation(self, validation: ValidationResult, *, operation: str) -> ValidationResult:
        metrics = validation.metrics or {}
        violation_total = 0
        for key, value in metrics.items():
            if not key.startswith("violations."):
                continue
            if isinstance(value, (int, float)) and value > 0:
                violation_total += int(value)

        if validation.errors or not validation.ok:
            reason = validation.errors[0] if validation.errors else None
            return ValidationResult(
                status="block",
                reason=reason,
                details={
                    "errors": list(validation.errors),
                    "warnings": list(validation.warnings),
                    "violations": violation_total or len(validation.errors),
                },
            )

        if violation_total > 0:
            reason = (
                validation.warnings[0]
                if validation.warnings
                else "Data-quality violations detected"
            )
            details: Dict[str, Any] = {
                "warnings": list(validation.warnings),
                "violations": violation_total,
            }
            if operation == "write":
                details.setdefault("operation", operation)
                return ValidationResult(
                    status="block",
                    reason=reason,
                    details=details,
                )
            return ValidationResult(
                status="warn",
                reason=reason,
                details=details,
            )

        if validation.warnings:
            return ValidationResult(
                status="warn",
                reason=validation.warnings[0],
                details={
                    "warnings": list(validation.warnings),
                    "violations": violation_total,
                },
            )

        return ValidationResult(status="ok", details={"violations": 0})

    def _record_pipeline_activity(
        self,
        *,
        contract: OpenDataContractStandard,
        dataset_id: str,
        dataset_version: str,
        operation: str,
        pipeline_context: Optional[Mapping[str, Any]],
        status: Optional[ValidationResult],
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
        dataset_entries = self._activity_log.setdefault(dataset_id, {})
        record = dataset_entries.get(dataset_version)
        if not isinstance(record, dict):
            record = {
                "dataset_id": dataset_id,
                "dataset_version": dataset_version,
                "contract_id": cid,
                "contract_version": cver,
                "events": [],
            }
        events = record.get("events")
        if not isinstance(events, list):
            events = []
        event = dict(entry)
        event.setdefault(
            "recorded_at",
            datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        )
        context_payload = event.get("pipeline_context")
        if isinstance(context_payload, Mapping):
            event["pipeline_context"] = dict(context_payload)
        elif context_payload is None:
            event["pipeline_context"] = {}
        events.append(event)
        record["events"] = events
        record["contract_id"] = cid
        record["contract_version"] = cver
        dataset_entries[dataset_version] = record

__all__ = ["LocalGovernanceServiceBackend"]
