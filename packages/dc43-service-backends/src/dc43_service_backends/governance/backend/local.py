"""In-process governance backend coordinating contract and quality services."""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any, Callable, Dict, Iterable, Mapping, Optional, Sequence, Tuple

from open_data_contract_standard.model import OpenDataContractStandard  # type: ignore

from dc43_service_backends.core.odcs import contract_identity
from dc43_service_backends.contracts import ContractServiceBackend, ContractStore
from dc43_service_backends.contracts.drafting import draft_from_validation_result
from dc43_service_backends.data_quality import DataQualityServiceBackend
from dc43_service_backends.data_products import (
    DataProductRegistrationResult,
    DataProductServiceBackend,
)
from dc43_service_backends.core.versioning import version_key
from dc43_service_clients.contracts import ContractServiceClient
from dc43_service_clients.data_quality import (
    DataQualityServiceClient,
    ObservationPayload,
    ValidationResult,
)
from dc43_service_clients.data_quality.transport import decode_validation_result
from dc43_service_clients.data_products import (
    DataProductInputBinding,
    DataProductOutputBinding,
    DataProductServiceClient,
    normalise_input_binding,
    normalise_output_binding,
)
from dc43_service_clients.odps import DataProductInputPort, DataProductOutputPort

from .interface import GovernanceServiceBackend
from ..storage import GovernanceStore, InMemoryGovernanceStore
from ..hooks import DatasetContractLinkHook
from dc43_service_clients.governance.lineage import OpenDataLineageEvent, encode_lineage_event
from dc43_service_clients.governance.models import (
    ContractReference,
    GovernanceReadContext,
    GovernanceWriteContext,
    GovernanceCredentials,
    PipelineContextSpec,
    QualityAssessment,
    QualityDraftContext,
    ResolvedReadPlan,
    ResolvedWritePlan,
    build_quality_context,
    derive_feedback,
    merge_pipeline_context,
)


logger = logging.getLogger(__name__)


def _latest_event(entry: Mapping[str, object]) -> Mapping[str, object]:
    events = entry.get("events") if isinstance(entry, Mapping) else None
    if isinstance(events, list):
        for event in reversed(events):
            if isinstance(event, Mapping):
                return dict(event)
    return {}


def _normalise_record_status(value: str | None) -> str:
    if not value:
        return "unknown"
    text = str(value).strip().lower()
    if not text:
        return "unknown"
    if text in {"ok", "pass", "passed", "success", "succeeded", "valid"}:
        return "ok"
    if text in {"warn", "warning", "caution"}:
        return "warn"
    if text in {"block", "blocked", "error", "fail", "failed", "invalid", "ko"}:
        return "block"
    if text in {"stale", "outdated", "expired"}:
        return "stale"
    return "unknown"


def _extract_violation_count(section: Mapping[str, object] | None) -> int:
    if not isinstance(section, Mapping):
        return 0

    total = 0
    candidate = section.get("violations")
    if isinstance(candidate, (int, float)):
        total = max(total, int(candidate))

    metrics = section.get("metrics")
    if isinstance(metrics, Mapping):
        for key, value in metrics.items():
            if str(key).startswith("violations") and isinstance(value, (int, float)):
                total = max(total, int(value))

    failed = section.get("failed_expectations")
    if isinstance(failed, Mapping):
        for info in failed.values():
            if not isinstance(info, Mapping):
                continue
            count = info.get("count")
            if isinstance(count, (int, float)):
                total = max(total, int(count))

    errors = section.get("errors")
    if isinstance(errors, list):
        total = max(total, len(errors))
    return total


def _as_validation_result(payload: object) -> ValidationResult | None:
    if payload is None:
        return None
    if isinstance(payload, ValidationResult):
        return payload
    if isinstance(payload, Mapping):
        try:
            return decode_validation_result(payload)
        except Exception:  # pragma: no cover - defensive guard
            logger.exception("Failed to decode validation payload")
            return None
    return None


class LocalGovernanceServiceBackend(GovernanceServiceBackend):
    """In-process orchestration across contract and data-quality services."""

    def __init__(
        self,
        *,
        contract_client: ContractServiceBackend | ContractServiceClient,
        dq_client: DataQualityServiceBackend | DataQualityServiceClient,
        data_product_client: DataProductServiceBackend | DataProductServiceClient | None = None,
        draft_store: ContractStore | None = None,
        link_hooks: Sequence[DatasetContractLinkHook] | None = None,
        store: GovernanceStore | None = None,
    ) -> None:
        self._contract_client = contract_client
        self._dq_client = dq_client
        self._data_product_client = data_product_client
        self._draft_store = draft_store
        self._credentials: Optional[GovernanceCredentials] = None
        self._link_hooks: tuple[DatasetContractLinkHook, ...] = (
            tuple(link_hooks) if link_hooks else ()
        )
        self._store: GovernanceStore = store or InMemoryGovernanceStore()

    # ------------------------------------------------------------------
    # Contract helpers
    # ------------------------------------------------------------------
    def get_contract(
        self,
        *,
        contract_id: str,
        contract_version: str,
    ) -> OpenDataContractStandard:
        return self._contract_client.get(contract_id, contract_version)

    def latest_contract(
        self,
        *,
        contract_id: str,
    ) -> Optional[OpenDataContractStandard]:
        try:
            return self._contract_client.latest(contract_id)
        except AttributeError:
            return None

    def list_contract_versions(self, *, contract_id: str) -> Sequence[str]:
        try:
            versions = self._contract_client.list_versions(contract_id)
        except AttributeError:
            return ()
        return tuple(versions)

    def describe_expectations(
        self,
        *,
        contract_id: str,
        contract_version: str,
    ) -> Sequence[Mapping[str, object]]:
        contract = self.get_contract(
            contract_id=contract_id,
            contract_version=contract_version,
        )
        try:
            expectations = self._dq_client.describe_expectations(contract=contract)
        except AttributeError:
            return ()
        return tuple(expectations)

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

        self._store.save_status(
            contract_id=contract_id,
            contract_version=contract_version,
            dataset_id=dataset_id,
            dataset_version=dataset_version,
            status=status,
        )

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

        return QualityAssessment(
            status=status,
            validation=validation,
            draft=draft,
            observations_reused=payload.reused,
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
        return self._store.load_status(
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
        try:
            self._contract_client.link_dataset_contract(
                dataset_id=dataset_id,
                dataset_version=dataset_version,
                contract_id=contract_id,
                contract_version=contract_version,
            )
        except AttributeError:
            pass
        self._store.link_dataset_contract(
            dataset_id=dataset_id,
            dataset_version=dataset_version,
            contract_id=contract_id,
            contract_version=contract_version,
        )
        for hook in self._link_hooks:
            hook(
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
        try:
            resolved = self._contract_client.get_linked_contract_version(
                dataset_id=dataset_id,
                dataset_version=dataset_version,
            )
        except AttributeError:
            resolved = None
        if resolved is not None:
            return resolved
        return self._store.get_linked_contract_version(
            dataset_id=dataset_id, dataset_version=dataset_version
        )

    def get_metrics(
        self,
        *,
        dataset_id: str,
        dataset_version: Optional[str] = None,
        contract_id: Optional[str] = None,
        contract_version: Optional[str] = None,
    ) -> Sequence[Mapping[str, object]]:
        return self._store.load_metrics(
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
    ) -> Sequence[Mapping[str, object]]:
        contract_filter = {str(item) for item in contract_ids or [] if str(item)}
        version_filter = [str(item) for item in dataset_versions or [] if str(item)]

        versions: list[str] = []
        if version_filter:
            versions = list(dict.fromkeys(version_filter))
        else:
            try:
                activity = self._store.load_pipeline_activity(dataset_id=dataset_id)
            except Exception:  # pragma: no cover - defensive guard for legacy stores
                logger.exception(
                    "Failed to load pipeline activity for %s", dataset_id
                )
                activity = ()
            seen_versions: set[str] = set()
            for entry in activity:
                candidate = str(entry.get("dataset_version") or "")
                if candidate and candidate not in seen_versions:
                    seen_versions.add(candidate)
                    versions.append(candidate)

        combos: set[tuple[str, str, str]] = set()
        for dataset_version in versions:
            try:
                events = self._store.load_pipeline_activity(
                    dataset_id=dataset_id,
                    dataset_version=dataset_version,
                )
            except Exception:  # pragma: no cover - defensive guard for legacy stores
                logger.exception(
                    "Failed to load pipeline events for %s@%s", dataset_id, dataset_version
                )
                continue
            for event in events:
                contract_id = str(event.get("contract_id") or "")
                contract_version = str(event.get("contract_version") or "")
                if contract_id and contract_version:
                    combos.add((dataset_version, contract_id, contract_version))
            linked = self._store.get_linked_contract_version(
                dataset_id=dataset_id,
                dataset_version=dataset_version,
            )
            if linked:
                contract_id, _, contract_version = str(linked).partition(":")
                if contract_id and contract_version:
                    combos.add((dataset_version, contract_id, contract_version))

        if not combos and versions:
            for dataset_version in versions:
                linked = self._store.get_linked_contract_version(
                    dataset_id=dataset_id,
                    dataset_version=dataset_version,
                )
                if linked:
                    contract_id, _, contract_version = str(linked).partition(":")
                    if contract_id and contract_version:
                        combos.add((dataset_version, contract_id, contract_version))

        sorted_combos = sorted(
            combos,
            key=lambda item: (
                version_key(item[0]),
                item[1],
                version_key(item[2]),
            ),
        )

        status_lookup: dict[tuple[str, str, str], ValidationResult | Mapping[str, object] | None] = {}
        try:
            status_records = self._store.load_status_matrix_entries(
                dataset_id=dataset_id,
                dataset_versions=versions,
                contract_ids=[item[1] for item in sorted(contract_filter or set())],
            )
        except AttributeError:
            status_records = ()
        except Exception:  # pragma: no cover - defensive guard for legacy stores
            logger.exception(
                "Failed to load batch statuses for %s", dataset_id,
            )
            status_records = ()
        if status_records is None:
            status_records = ()
        for entry in status_records:
            if not isinstance(entry, Mapping):
                continue
            dataset_version = str(entry.get("dataset_version") or "")
            contract_id = str(entry.get("contract_id") or "")
            contract_version = str(entry.get("contract_version") or "")
            if not (dataset_version and contract_id and contract_version):
                continue
            status_lookup[(dataset_version, contract_id, contract_version)] = entry.get(
                "status"
            )

        results: list[Mapping[str, object]] = []
        for dataset_version, contract_id, contract_version in sorted_combos:
            if contract_filter and contract_id not in contract_filter:
                continue
            status = status_lookup.get(
                (dataset_version, contract_id, contract_version)
            )
            if status is None:
                try:
                    status = self._store.load_status(
                        contract_id=contract_id,
                        contract_version=contract_version,
                        dataset_id=dataset_id,
                        dataset_version=dataset_version,
                    )
                except Exception:  # pragma: no cover - defensive guard for legacy stores
                    logger.exception(
                        "Failed to load status for %s@%s via %s:%s",
                        dataset_id,
                        dataset_version,
                        contract_id,
                        contract_version,
                    )
                    status = None
            results.append(
                {
                    "dataset_id": dataset_id,
                    "dataset_version": dataset_version,
                    "contract_id": contract_id,
                    "contract_version": contract_version,
                    "status": status,
                }
            )
        return tuple(results)

    def list_datasets(self) -> Sequence[str]:
        return self._store.list_datasets()

    def get_dataset_records(
        self,
        *,
        dataset_id: str | None = None,
        dataset_version: str | None = None,
    ) -> Sequence[Mapping[str, object]]:
        dataset_ids = [dataset_id] if dataset_id else self.list_datasets()
        records: list[Mapping[str, object]] = []
        record_indices: dict[tuple[str, str, str, str], int] = {}
        for dataset_name in dataset_ids:
            if not dataset_name:
                continue
            version_filter = dataset_version if dataset_id else None
            activity = self.get_pipeline_activity(
                dataset_id=dataset_name,
                dataset_version=version_filter,
                include_status=True,
            )
            inline_status_available = any(
                isinstance(item, Mapping) and item.get("validation_status") is not None
                for item in activity
            )
            status_lookup: dict[tuple[str, str, str], ValidationResult | None] = {}
            if activity and not inline_status_available:
                contract_candidates = {
                    str(item.get("contract_id") or "").strip()
                    for item in activity
                    if isinstance(item, Mapping) and item.get("contract_id")
                }
                version_candidates: set[str] = set()
                if version_filter:
                    version_candidates.add(version_filter)
                else:
                    version_candidates = {
                        value
                        for value in (
                            str(item.get("dataset_version") or "").strip()
                            for item in activity
                            if isinstance(item, Mapping) and item.get("dataset_version")
                        )
                        if value and value.lower() != "latest"
                    }
                matrix_entries = self.get_status_matrix(
                    dataset_id=dataset_name,
                    contract_ids=[c for c in contract_candidates if c],
                    dataset_versions=[v for v in version_candidates if v],
                )
                for entry in matrix_entries:
                    if isinstance(entry, Mapping):
                        cid = str(entry.get("contract_id") or "").strip()
                        cver = str(entry.get("contract_version") or "").strip()
                        dver = str(entry.get("dataset_version") or "").strip()
                        status_obj = entry.get("status")
                    else:
                        cid = getattr(entry, "contract_id", "") or ""
                        cver = getattr(entry, "contract_version", "") or ""
                        dver = getattr(entry, "dataset_version", "") or ""
                        status_obj = getattr(entry, "status", None)
                    if cid and cver and dver:
                        status_lookup[(cid, cver, dver)] = _as_validation_result(status_obj)
            for raw_entry in activity:
                if not isinstance(raw_entry, Mapping):
                    continue
                dataset_version_value = str(raw_entry.get("dataset_version") or "").strip()
                if (
                    version_filter
                    and dataset_version_value
                    and dataset_version_value != version_filter
                ):
                    continue
                contract_id = str(raw_entry.get("contract_id") or "").strip()
                contract_version = str(raw_entry.get("contract_version") or "").strip()

                latest_event = _latest_event(raw_entry)
                validation: ValidationResult | None = None
                inline_payload = raw_entry.get("validation_status")
                if inline_payload is not None:
                    validation = _as_validation_result(inline_payload)
                elif contract_id and contract_version and dataset_version_value:
                    validation = status_lookup.get(
                        (contract_id, contract_version, dataset_version_value)
                    )
                    if (
                        validation is None
                        and not inline_status_available
                        and dataset_version_value
                        and dataset_version_value.lower() != "latest"
                    ):
                        validation = _as_validation_result(
                            self.get_status(
                                contract_id=contract_id,
                                contract_version=contract_version,
                                dataset_id=dataset_name,
                                dataset_version=dataset_version_value,
                            )
                        )

                details: dict[str, object] = {}
                reason = ""
                status_value = "unknown"
                observation_operation = ""
                observation_scope = ""
                observation_label = ""
                if validation is not None:
                    try:
                        status_value = _normalise_record_status(validation.status)
                        details = dict(validation.details)
                        reason = validation.reason or ""
                    except Exception:  # pragma: no cover - defensive guard
                        logger.exception(
                            "Failed to interpret validation status for %s@%s",
                            dataset_name,
                            dataset_version_value,
                        )
                        details = {}
                else:
                    raw_status = str(latest_event.get("dq_status", "unknown"))
                    status_value = _normalise_record_status(raw_status)

                if isinstance(details, Mapping):
                    observation_operation = str(details.get("observation_operation") or "")
                    observation_scope = str(details.get("observation_scope") or "")
                    observation_label = str(details.get("observation_label") or "")
                    if observation_scope and not observation_label:
                        observation_label = observation_scope.replace("_", " ").title()

                pipeline_context = latest_event.get("pipeline_context")
                run_type = "infer"
                scenario_key = None
                if isinstance(pipeline_context, Mapping):
                    run_type = str(pipeline_context.get("run_type", run_type))
                    scenario_key_value = pipeline_context.get("scenario_key")
                    if isinstance(scenario_key_value, str) and scenario_key_value:
                        scenario_key = scenario_key_value
                if latest_event.get("scenario_key") and not scenario_key:
                    scenario_key = str(latest_event.get("scenario_key"))

                draft_version = details.get("draft_contract_version")
                if not isinstance(draft_version, str):
                    draft_version = latest_event.get("draft_contract_version")
                    if not isinstance(draft_version, str):
                        draft_version = None

                data_product_id = ""
                data_product_port = ""
                data_product_role = ""
                data_product_info = latest_event.get("data_product")
                if isinstance(data_product_info, Mapping):
                    data_product_id = str(data_product_info.get("id") or "")
                    data_product_port = str(data_product_info.get("port") or "")
                    data_product_role = str(data_product_info.get("role") or "")
                else:
                    if latest_event.get("data_product_id"):
                        data_product_id = str(latest_event.get("data_product_id"))
                    if latest_event.get("data_product_port"):
                        data_product_port = str(latest_event.get("data_product_port"))
                    if latest_event.get("data_product_role"):
                        data_product_role = str(latest_event.get("data_product_role"))

                violations = _extract_violation_count(details)

                payload: dict[str, object] = {
                    "contract_id": contract_id,
                    "contract_version": contract_version,
                    "dataset_name": dataset_name,
                    "dataset_version": dataset_version_value,
                    "status": status_value,
                    "dq_details": details,
                    "run_type": run_type,
                    "violations": violations,
                    "draft_contract_version": draft_version,
                    "scenario_key": scenario_key,
                    "data_product_id": data_product_id,
                    "data_product_port": data_product_port,
                    "data_product_role": data_product_role,
                    "observation_operation": observation_operation,
                    "observation_scope": observation_scope,
                    "observation_label": observation_label,
                    "reason": reason,
                }

                dedup_key: tuple[str, str, str, str] | None = None
                if dataset_name and dataset_version_value and contract_id and contract_version:
                    dedup_key = (
                        dataset_name,
                        dataset_version_value,
                        contract_id,
                        contract_version,
                    )

                if dedup_key is None:
                    records.append(payload)
                else:
                    existing_index = record_indices.get(dedup_key)
                    if existing_index is None:
                        record_indices[dedup_key] = len(records)
                        records.append(payload)
                    else:
                        records[existing_index] = payload

        return tuple(records)

    def get_pipeline_activity(
        self,
        *,
        dataset_id: str,
        dataset_version: Optional[str] = None,
        include_status: bool = False,
    ) -> Sequence[Mapping[str, Any]]:
        records = self._store.load_pipeline_activity(
            dataset_id=dataset_id, dataset_version=dataset_version
        )
        if not include_status:
            return records

        entries: list[Mapping[str, Any]] = []
        combos: list[tuple[str, str, str]] = []
        versions: set[str] = set()
        contracts: set[str] = set()
        for record in records:
            if not isinstance(record, Mapping):
                continue
            entry = dict(record)
            entries.append(entry)
            contract_id = str(entry.get("contract_id") or "")
            contract_version = str(entry.get("contract_version") or "")
            recorded_version = str(
                entry.get("dataset_version") or dataset_version or ""
            )
            if contract_id and contract_version and recorded_version:
                combos.append((recorded_version, contract_id, contract_version))
                versions.add(recorded_version)
                contracts.add(contract_id)

        status_lookup: dict[tuple[str, str, str], ValidationResult | Mapping[str, object] | None] = {}
        if combos:
            try:
                status_records = self._store.load_status_matrix_entries(
                    dataset_id=dataset_id,
                    dataset_versions=list(versions),
                    contract_ids=list(contracts),
                )
            except AttributeError:
                status_records = ()
            except Exception:  # pragma: no cover - defensive guard for legacy stores
                logger.exception(
                    "Failed to load batch statuses for %s", dataset_id,
                )
                status_records = ()
            if status_records is None:
                status_records = ()
            for entry in status_records:
                if not isinstance(entry, Mapping):
                    continue
                dataset_version_value = str(entry.get("dataset_version") or "")
                contract_id_value = str(entry.get("contract_id") or "")
                contract_version_value = str(entry.get("contract_version") or "")
                if not (
                    dataset_version_value and contract_id_value and contract_version_value
                ):
                    continue
                status_lookup[
                    (
                        dataset_version_value,
                        contract_id_value,
                        contract_version_value,
                    )
                ] = entry.get("status")

        enriched: list[Mapping[str, Any]] = []
        for entry in entries:
            contract_id = str(entry.get("contract_id") or "")
            contract_version = str(entry.get("contract_version") or "")
            recorded_version = str(
                entry.get("dataset_version") or dataset_version or ""
            )
            status_payload = None
            if contract_id and contract_version and recorded_version:
                status_payload = status_lookup.get(
                    (recorded_version, contract_id, contract_version)
                )
                if status_payload is None:
                    try:
                        status_payload = self._store.load_status(
                            contract_id=contract_id,
                            contract_version=contract_version,
                            dataset_id=dataset_id,
                            dataset_version=recorded_version,
                        )
                    except Exception:  # pragma: no cover - defensive guard for legacy stores
                        logger.exception(
                            "Failed to load status for %s@%s via %s:%s",
                            dataset_id,
                            recorded_version,
                            contract_id,
                            contract_version,
                        )
            if status_payload is not None:
                entry["validation_status"] = status_payload
            enriched.append(entry)
        return tuple(enriched)

    def resolve_read_context(
        self,
        *,
        context: GovernanceReadContext,
    ) -> ResolvedReadPlan:
        status_options = self._normalise_status_options(
            self._status_options_from_context(context)
        )
        contract, contract_id, contract_version = self._resolve_contract_spec(
            contract_reference=context.contract,
            input_binding=context.input_binding,
            output_binding=None,
            input_status_options=status_options,
        )
        dataset_id = context.dataset_id or contract.id or contract_id
        dataset_version = context.dataset_version or contract.version or contract_version
        pipeline = merge_pipeline_context(context.pipeline_context)
        return ResolvedReadPlan(
            contract=contract,
            contract_id=contract_id,
            contract_version=contract_version,
            dataset_id=dataset_id,
            dataset_version=dataset_version,
            dataset_format=context.dataset_format,
            input_binding=context.input_binding,
            pipeline_context=pipeline,
            bump=context.bump,
            draft_on_violation=context.draft_on_violation,
            allowed_data_product_statuses=status_options["allowed_statuses"],
            allow_missing_data_product_status=status_options["allow_missing"],
            data_product_status_case_insensitive=status_options["case_insensitive"],
            data_product_status_failure_message=status_options["failure_message"],
            enforce_data_product_status=status_options["enforce"],
        )

    def resolve_write_context(
        self,
        *,
        context: GovernanceWriteContext,
    ) -> ResolvedWritePlan:
        status_options = self._normalise_status_options(
            self._status_options_from_context(context)
        )
        contract, contract_id, contract_version = self._resolve_contract_spec(
            contract_reference=context.contract,
            input_binding=None,
            output_binding=context.output_binding,
            output_status_options=status_options,
        )
        dataset_id = context.dataset_id or contract.id or contract_id
        dataset_version = context.dataset_version or contract.version or contract_version
        pipeline = merge_pipeline_context(context.pipeline_context)
        return ResolvedWritePlan(
            contract=contract,
            contract_id=contract_id,
            contract_version=contract_version,
            dataset_id=dataset_id,
            dataset_version=dataset_version,
            dataset_format=context.dataset_format,
            output_binding=context.output_binding,
            pipeline_context=pipeline,
            bump=context.bump,
            draft_on_violation=context.draft_on_violation,
            allowed_data_product_statuses=status_options["allowed_statuses"],
            allow_missing_data_product_status=status_options["allow_missing"],
            data_product_status_case_insensitive=status_options["case_insensitive"],
            data_product_status_failure_message=status_options["failure_message"],
            enforce_data_product_status=status_options["enforce"],
        )

    def evaluate_read_plan(
        self,
        *,
        plan: ResolvedReadPlan,
        validation: ValidationResult | None,
        observations: Callable[[], ObservationPayload],
    ) -> QualityAssessment:
        return self.evaluate_dataset(
            contract_id=plan.contract_id,
            contract_version=plan.contract_version,
            dataset_id=plan.dataset_id,
            dataset_version=plan.dataset_version,
            validation=validation,
            observations=observations,
            bump=plan.bump,
            pipeline_context=plan.pipeline_context,
            operation="read",
            draft_on_violation=plan.draft_on_violation,
        )

    def evaluate_write_plan(
        self,
        *,
        plan: ResolvedWritePlan,
        validation: ValidationResult | None,
        observations: Callable[[], ObservationPayload],
    ) -> QualityAssessment:
        return self.evaluate_dataset(
            contract_id=plan.contract_id,
            contract_version=plan.contract_version,
            dataset_id=plan.dataset_id,
            dataset_version=plan.dataset_version,
            validation=validation,
            observations=observations,
            bump=plan.bump,
            pipeline_context=plan.pipeline_context,
            operation="write",
            draft_on_violation=plan.draft_on_violation,
        )

    def register_read_activity(
        self,
        *,
        plan: ResolvedReadPlan,
        assessment: QualityAssessment,
    ) -> None:
        self._register_input_binding(plan=plan)

    def register_write_activity(
        self,
        *,
        plan: ResolvedWritePlan,
        assessment: QualityAssessment,
    ) -> None:
        self.link_dataset_contract(
            dataset_id=plan.dataset_id,
            dataset_version=plan.dataset_version,
            contract_id=plan.contract_id,
            contract_version=plan.contract_version,
        )
        self._register_output_binding(plan=plan)

    def publish_lineage_event(
        self,
        *,
        event: OpenDataLineageEvent,
    ) -> None:
        def _as_mapping(value: object) -> Mapping[str, object]:
            return value if isinstance(value, Mapping) else {}

        def _as_str(value: object | None) -> str | None:
            if value is None:
                return None
            text = str(value).strip()
            return text or None

        payload = encode_lineage_event(event)
        dataset_entries: Sequence[Mapping[str, object]]
        dataset_entries = list(payload.get("inputs") or []) or list(payload.get("outputs") or [])
        if not dataset_entries:
            logger.warning("Ignoring lineage event without dataset entries")
            return

        dataset_entry = dataset_entries[0]
        dataset_facets = _as_mapping(dataset_entry.get("facets"))
        dataset_info = _as_mapping(dataset_facets.get("dc43Dataset"))
        operation = _as_str(dataset_info.get("operation"))
        if operation is None:
            operation = "read" if payload.get("inputs") else "write"
        operation = operation.lower()

        dataset_id = _as_str(dataset_info.get("datasetId") or dataset_entry.get("name"))
        version_facet = _as_mapping(dataset_facets.get("version"))
        dataset_version = _as_str(
            dataset_info.get("datasetVersion") or version_facet.get("datasetVersion")
        )
        format_facet = _as_mapping(dataset_facets.get("dc43Format"))
        dataset_format = _as_str(format_facet.get("format"))

        contract_facet = _as_mapping(dataset_facets.get("dc43Contract"))
        contract_id = _as_str(contract_facet.get("contractId"))
        contract_version = _as_str(contract_facet.get("contractVersion"))

        if not contract_id or not contract_version or not dataset_id:
            logger.warning(
                "Lineage event missing identifiers; skipping registration: dataset=%s version=%s contract=%s:%s",
                dataset_id,
                dataset_version,
                contract_id,
                contract_version,
            )
            return

        try:
            contract = self._contract_client.get(contract_id, contract_version)
        except Exception:  # pragma: no cover - safety net for misconfigured stores
            logger.exception(
                "Failed to load contract %s:%s for lineage event", contract_id, contract_version
            )
            return

        resolved_contract_id = contract.id or contract_id
        resolved_contract_version = contract.version or contract_version
        dataset_version_value = dataset_version or ""

        binding_spec = dataset_facets.get("dc43DataProduct")
        binding_mapping = _as_mapping(binding_spec)

        run_payload = _as_mapping(payload.get("run"))
        run_facets = _as_mapping(run_payload.get("facets"))
        context_facet = _as_mapping(run_facets.get("dc43PipelineContext"))
        pipeline_context = merge_pipeline_context(context_facet.get("context"))

        validation_facet = _as_mapping(run_facets.get("dc43Validation"))
        validation_result: ValidationResult | None = None
        observations_reused = False
        if validation_facet:
            errors_raw = validation_facet.get("errors")
            warnings_raw = validation_facet.get("warnings")
            metrics_raw = validation_facet.get("metrics")
            schema_raw = validation_facet.get("schema")
            details_raw = validation_facet.get("details")
            metrics = dict(metrics_raw) if isinstance(metrics_raw, Mapping) else None
            schema: dict[str, Mapping[str, object]] | None = None
            if isinstance(schema_raw, Mapping):
                schema = {
                    str(key): dict(value) if isinstance(value, Mapping) else {}
                    for key, value in schema_raw.items()
                }
            details = _as_mapping(details_raw)
            validation_result = ValidationResult(
                ok=bool(validation_facet.get("ok", True)),
                errors=[str(item) for item in errors_raw] if isinstance(errors_raw, Sequence) else None,
                warnings=[str(item) for item in warnings_raw] if isinstance(warnings_raw, Sequence) else None,
                metrics=metrics,
                schema=schema,
                status=str(validation_facet.get("status") or "unknown"),
                reason=_as_str(validation_facet.get("reason")),
                details=details,
            )
            reused_flag = validation_facet.get("reused")
            if reused_flag is None:
                reused_flag = details.get("reused")
            if isinstance(reused_flag, bool):
                observations_reused = reused_flag
            elif reused_flag is not None:
                observations_reused = bool(reused_flag)

        assessment = QualityAssessment(
            status=validation_result,
            validation=validation_result,
            observations_reused=observations_reused,
        )

        if dataset_version:
            try:
                self.link_dataset_contract(
                    dataset_id=dataset_id,
                    dataset_version=dataset_version,
                    contract_id=resolved_contract_id,
                    contract_version=resolved_contract_version,
                )
            except Exception:  # pragma: no cover - store exceptions should not abort processing
                logger.exception(
                    "Failed to link dataset %s@%s to contract %s:%s from lineage event",
                    dataset_id,
                    dataset_version,
                    resolved_contract_id,
                    resolved_contract_version,
                )

        output_binding: DataProductOutputBinding | None = None

        if operation == "read":
            input_binding = normalise_input_binding(binding_mapping) if binding_mapping else None
            plan = ResolvedReadPlan(
                contract=contract,
                contract_id=resolved_contract_id,
                contract_version=resolved_contract_version,
                dataset_id=dataset_id,
                dataset_version=dataset_version_value,
                dataset_format=dataset_format,
                input_binding=input_binding,
                pipeline_context=pipeline_context,
            )
            try:
                self.register_read_activity(plan=plan, assessment=assessment)
            except Exception:  # pragma: no cover - defensive
                logger.exception(
                    "Failed to register read lineage activity for dataset %s", dataset_id
                )
        else:
            output_binding = (
                normalise_output_binding(binding_mapping) if binding_mapping else None
            )
            plan = ResolvedWritePlan(
                contract=contract,
                contract_id=resolved_contract_id,
                contract_version=resolved_contract_version,
                dataset_id=dataset_id,
                dataset_version=dataset_version_value,
                dataset_format=dataset_format,
                output_binding=output_binding,
                pipeline_context=pipeline_context,
            )
            try:
                self.register_write_activity(plan=plan, assessment=assessment)
            except Exception:  # pragma: no cover - defensive
                logger.exception(
                    "Failed to register write lineage activity for dataset %s", dataset_id
                )

        self._record_pipeline_activity(
            contract=contract,
            dataset_id=dataset_id,
            dataset_version=dataset_version_value,
            operation=operation,
            pipeline_context=pipeline_context,
            status=validation_result,
            observations_reused=observations_reused,
            lineage_event=payload,
        )

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------
    def _resolve_contract_spec(
        self,
        *,
        contract_reference: ContractReference | None,
        input_binding: Any,
        output_binding: Any,
        input_status_options: Optional[Mapping[str, object]] = None,
        output_status_options: Optional[Mapping[str, object]] = None,
    ) -> Tuple[OpenDataContractStandard, str, str]:
        if contract_reference is not None:
            return self._resolve_contract_reference(contract_reference)
        if input_binding is not None:
            contract_id, contract_version = self._resolve_contract_from_input(
                input_binding,
                status_options=input_status_options,
            )
            contract = self._contract_client.get(contract_id, contract_version)
            return contract, contract_id, contract.version or contract_version
        if output_binding is not None:
            contract_id, contract_version = self._resolve_contract_from_output(
                output_binding,
                status_options=output_status_options,
            )
            contract = self._contract_client.get(contract_id, contract_version)
            return contract, contract_id, contract.version or contract_version
        raise ValueError("A contract reference or data-product binding is required")

    def _resolve_contract_reference(
        self, reference: ContractReference
    ) -> Tuple[OpenDataContractStandard, str, str]:
        version = reference.resolved_version()
        if version:
            contract = self._contract_client.get(reference.contract_id, version)
            return contract, contract.id or reference.contract_id, contract.version or version
        latest = self.latest_contract(contract_id=reference.contract_id)
        if latest is None:
            raise ValueError(f"No contract found for id '{reference.contract_id}'")
        return latest, latest.id or reference.contract_id, latest.version

    def _resolve_contract_from_input(
        self,
        binding: DataProductInputBinding,
        *,
        status_options: Optional[Mapping[str, object]] = None,
    ) -> Tuple[str, str]:
        client = self._require_data_product_client()
        port_name = binding.port_name or binding.source_output_port
        if not port_name:
            raise ValueError("Input binding requires a port_name or source_output_port")

        options = self._normalise_status_options(status_options)
        data_product_id = binding.data_product
        source_product_id = binding.source_data_product
        source_port_name = binding.source_output_port or port_name
        source_version_spec = binding.source_data_product_version
        source_contract_spec = binding.source_contract_version

        product = self._load_product_for_operation(
            data_product_id=data_product_id,
            version_spec=binding.data_product_version,
            operation="read",
            subject="Data product",
            status_options=options,
        )
        if product is not None:
            port = product.find_input_port(port_name)
            if port and port.contract_id and port.version:
                if source_contract_spec:
                    target_id = source_product_id or data_product_id or ""
                    self._enforce_version_constraint(
                        expected=source_contract_spec,
                        actual=port.version,
                        data_product_id=target_id,
                        subject="Output port contract",
                    )
                return port.contract_id, port.version

        if source_product_id:
            resolved = client.resolve_output_contract(
                data_product_id=source_product_id,
                port_name=source_port_name,
            )
            if resolved:
                source_product = self._load_product_for_operation(
                    data_product_id=source_product_id,
                    version_spec=source_version_spec,
                    operation="read",
                    subject="Source data product",
                    status_options=options,
                )
                if source_product is None and source_version_spec:
                    raise ValueError(
                        f"Source data product {source_product_id} version {source_version_spec} could not be retrieved"
                    )
                self._enforce_version_constraint(
                    expected=source_contract_spec,
                    actual=resolved[1],
                    data_product_id=source_product_id,
                    subject="Output port contract",
                )
                return resolved

            source_product = self._load_product_for_operation(
                data_product_id=source_product_id,
                version_spec=source_version_spec,
                operation="read",
                subject="Source data product",
                status_options=options,
            )
            if source_product is not None:
                port = source_product.find_output_port(source_port_name)
                if port and port.contract_id and port.version:
                    self._enforce_version_constraint(
                        expected=source_contract_spec,
                        actual=port.version,
                        data_product_id=source_product_id,
                        subject="Output port contract",
                    )
                    return port.contract_id, port.version

        if product is not None:
            port = product.find_input_port(port_name)
            if port and port.contract_id and port.version:
                return port.contract_id, port.version

        raise ValueError("Unable to resolve contract from input binding")

    def _resolve_contract_from_output(
        self,
        binding: DataProductOutputBinding,
        *,
        status_options: Optional[Mapping[str, object]] = None,
    ) -> Tuple[str, str]:
        client = self._require_data_product_client()
        port_name = binding.port_name or "default"

        options = self._normalise_status_options(status_options)
        product = self._load_product_for_operation(
            data_product_id=binding.data_product,
            version_spec=binding.data_product_version,
            operation="write",
            subject="Data product",
            status_options=options,
        )
        if product is not None:
            port = product.find_output_port(port_name)
            if port and port.contract_id and port.version:
                return port.contract_id, port.version

        resolved = client.resolve_output_contract(
            data_product_id=binding.data_product,
            port_name=port_name,
        )
        if resolved:
            if product is None and binding.data_product_version:
                product = self._load_product_for_operation(
                    data_product_id=binding.data_product,
                    version_spec=binding.data_product_version,
                    operation="write",
                    subject="Data product",
                    status_options=options,
                )
            actual_version = product.version if product is not None else None
            self._enforce_version_constraint(
                expected=binding.data_product_version,
                actual=actual_version,
                data_product_id=binding.data_product,
                subject="Data product",
            )
            return resolved

        if product is not None:
            port = product.find_output_port(port_name)
            if port and port.contract_id and port.version:
                return port.contract_id, port.version

        raise ValueError("Unable to resolve contract from output binding")

    def _require_data_product_client(
        self,
    ) -> DataProductServiceBackend | DataProductServiceClient:
        if self._data_product_client is None:
            raise RuntimeError("Data product service is not configured")
        return self._data_product_client

    def _data_product_latest(self, data_product_id: str) -> Any:
        client = self._data_product_client
        if client is None:
            return None
        try:
            return client.latest(data_product_id)
        except AttributeError:
            return None

    def _register_input_binding(self, *, plan: ResolvedReadPlan) -> None:
        binding = plan.input_binding
        client = self._data_product_client
        if binding is None or client is None:
            return
        port_name = binding.port_name or binding.source_output_port or "default"
        options = self._status_options_from_plan(plan)
        requirement = self._normalise_version_spec(binding.data_product_version)
        pinned_version = requirement is not None and not requirement.startswith(">=")

        registration: Optional[DataProductRegistrationResult]
        if pinned_version:
            registration = None
        else:
            try:
                register = client.register_input_port
            except AttributeError:
                return
            port = DataProductInputPort(
                name=port_name,
                version=plan.contract_version,
                contract_id=plan.contract_id,
            )
            try:
                registration = register(
                    data_product_id=binding.data_product,
                    port=port,
                    bump=binding.bump,
                    custom_properties=binding.custom_properties,
                    source_data_product=binding.source_data_product,
                    source_output_port=binding.source_output_port,
                )
            except TypeError:
                registration = register(
                    data_product_id=binding.data_product,
                    port_name=port_name,
                    contract_id=plan.contract_id,
                    contract_version=plan.contract_version,
                    bump=binding.bump,
                    custom_properties=binding.custom_properties,
                    source_data_product=binding.source_data_product,
                    source_output_port=binding.source_output_port,
                )
        self._raise_if_registration_requires_review(
            registration,
            data_product=binding.data_product,
            port_name=port_name,
            binding_type="input",
        )

        product = self._registration_product_for_binding(
            binding=binding,
            registration=registration,
        )
        self._enforce_input_product_constraints(
            binding=binding,
            port_name=port_name,
            product=product,
            status_options=options,
        )

    def _register_output_binding(self, *, plan: ResolvedWritePlan) -> None:
        binding = plan.output_binding
        client = self._data_product_client
        if binding is None or client is None:
            return
        port_name = binding.port_name or "default"
        options = self._status_options_from_plan(plan)
        requirement = self._normalise_version_spec(binding.data_product_version)
        pinned_version = requirement is not None and not requirement.startswith(">=")

        registration: Optional[DataProductRegistrationResult]
        if pinned_version:
            registration = None
        else:
            try:
                register = client.register_output_port
            except AttributeError:
                return
            port = DataProductOutputPort(
                name=port_name,
                version=plan.contract_version,
                contract_id=plan.contract_id,
            )
            try:
                registration = register(
                    data_product_id=binding.data_product,
                    port=port,
                    bump=binding.bump,
                    custom_properties=binding.custom_properties,
                )
            except TypeError:
                registration = register(
                    data_product_id=binding.data_product,
                    port_name=port_name,
                    contract_id=plan.contract_id,
                    contract_version=plan.contract_version,
                    bump=binding.bump,
                    custom_properties=binding.custom_properties,
                )
        self._raise_if_registration_requires_review(
            registration,
            data_product=binding.data_product,
            port_name=port_name,
            binding_type="output",
        )

        product = self._registration_product_for_binding(
            binding=binding,
            registration=registration,
        )
        self._enforce_output_product_constraints(
            binding=binding,
            port_name=port_name,
            product=product,
            status_options=options,
        )

    def _raise_if_registration_requires_review(
        self,
        registration: Optional[DataProductRegistrationResult],
        *,
        data_product: str,
        port_name: str,
        binding_type: str,
    ) -> None:
        if registration is None or not registration.changed:
            return
        product = registration.product
        version = product.version if product is not None else "<unknown>"
        raw_status = product.status if product is not None else ""
        status = (raw_status or "").lower()
        if status != "draft":
            raise RuntimeError(
                f"Data product {binding_type} registration did not produce a draft version"
            )
        raise RuntimeError(
            f"Data product {data_product} {binding_type} port {port_name} requires review "
            f"at version {version}"
        )

    def _normalise_version_spec(self, spec: Optional[str]) -> Optional[str]:
        if spec is None:
            return None
        value = str(spec).strip()
        if not value:
            return None
        if value.startswith("=="):
            return value[2:].strip() or None
        return value

    def _registration_product_for_binding(
        self,
        *,
        binding: DataProductInputBinding | DataProductOutputBinding,
        registration: Optional[DataProductRegistrationResult],
    ) -> Any:
        data_product_id = binding.data_product
        if not data_product_id:
            return registration.product if registration else None

        version_spec = binding.data_product_version
        requirement = self._normalise_version_spec(version_spec)
        if requirement and not requirement.startswith(">="):
            return self._load_data_product(
                data_product_id=data_product_id,
                version_spec=requirement,
            )

        product = registration.product if registration else None
        if product is not None:
            return product

        return self._load_data_product(
            data_product_id=data_product_id,
            version_spec=version_spec,
        )

    def _version_satisfies(self, expected: Optional[str], actual: Optional[str]) -> bool:
        if expected is None or not expected.strip():
            return True
        if not actual:
            return False
        requirement = expected.strip()
        if requirement.startswith("=="):
            target = requirement[2:].strip()
            return not target or actual == target
        if requirement.startswith(">="):
            target = requirement[2:].strip()
            if not target:
                return True
            return version_key(actual) >= version_key(target)
        return actual == requirement

    def _enforce_version_constraint(
        self,
        *,
        expected: Optional[str],
        actual: Optional[str],
        data_product_id: str,
        subject: str,
    ) -> None:
        if self._version_satisfies(expected, actual):
            return
        raise ValueError(
            f"{subject} version {actual or '<unknown>'} does not satisfy {expected} "
            f"for data product {data_product_id}"
        )

    def _status_options_from_context(
        self,
        context: GovernanceReadContext | GovernanceWriteContext,
    ) -> Mapping[str, object]:
        return {
            "allowed_statuses": context.allowed_data_product_statuses,
            "allow_missing": context.allow_missing_data_product_status,
            "case_insensitive": context.data_product_status_case_insensitive,
            "failure_message": context.data_product_status_failure_message,
            "enforce": context.enforce_data_product_status,
        }

    def _status_options_from_plan(
        self,
        plan: ResolvedReadPlan | ResolvedWritePlan,
    ) -> Mapping[str, object]:
        return {
            "allowed_statuses": plan.allowed_data_product_statuses,
            "allow_missing": plan.allow_missing_data_product_status,
            "case_insensitive": plan.data_product_status_case_insensitive,
            "failure_message": plan.data_product_status_failure_message,
            "enforce": plan.enforce_data_product_status,
        }

    def _normalise_status_options(
        self,
        options: Optional[Mapping[str, object]],
    ) -> Dict[str, object]:
        allowed_raw = None if options is None else options.get("allowed_statuses")
        if allowed_raw is None:
            allowed: Tuple[str, ...] = ("active",)
        elif isinstance(allowed_raw, str):
            allowed = (allowed_raw.strip(),) if allowed_raw.strip() else ()
        else:
            values: Iterable[str] = (
                str(value).strip()
                for value in allowed_raw  # type: ignore[arg-type]
                if value is not None
            )
            allowed = tuple(value for value in values if value)

        allow_missing_raw = None if options is None else options.get("allow_missing")
        allow_missing = bool(allow_missing_raw) if allow_missing_raw is not None else False

        case_insensitive_raw = None if options is None else options.get("case_insensitive")
        case_insensitive = (
            True if case_insensitive_raw is None else bool(case_insensitive_raw)
        )

        failure_message = None if options is None else options.get("failure_message")

        enforce_raw = None if options is None else options.get("enforce")
        enforce = True if enforce_raw is None else bool(enforce_raw)

        return {
            "allowed_statuses": allowed,
            "allow_missing": allow_missing,
            "case_insensitive": case_insensitive,
            "failure_message": failure_message,
            "enforce": enforce,
        }

    def _status_kwargs(self, options: Mapping[str, object]) -> Dict[str, object]:
        allowed = options.get("allowed_statuses")
        return {
            "allowed_statuses": allowed if allowed is not None else None,
            "allow_missing": bool(options.get("allow_missing", False)),
            "case_insensitive": bool(options.get("case_insensitive", True)),
            "failure_message": options.get("failure_message"),
            "enforce": bool(options.get("enforce", True)),
        }

    def _load_data_product(
        self,
        *,
        data_product_id: Optional[str],
        version_spec: Optional[str],
        default: Any = None,
    ) -> Any:
        if not data_product_id:
            return default
        client = self._data_product_client
        if client is None:
            return default
        requirement = version_spec.strip() if isinstance(version_spec, str) else ""
        direct_version = None
        if requirement and not requirement.startswith(">="):
            direct_version = self._normalise_version_spec(requirement)
        try:
            getter = client.get
        except AttributeError:
            getter = None
        if direct_version and getter is not None:
            try:
                return getter(data_product_id, direct_version)
            except Exception:
                raise ValueError(
                    f"Data product {data_product_id} version {direct_version} could not be retrieved"
                )

        latest = None
        try:
            latest = client.latest(data_product_id)
        except AttributeError:
            latest = None
        if latest and self._version_satisfies(requirement, latest.version):
            return latest

        versions: Sequence[str] = ()
        try:
            versions = list(client.list_versions(data_product_id))
        except AttributeError:
            versions = ()
        if requirement:
            candidates = [version for version in versions if version]
            for version in sorted(candidates, key=version_key, reverse=True):
                if not self._version_satisfies(requirement, version):
                    continue
                if getter is not None:
                    return getter(data_product_id, version)
        return latest or default

    def _load_product_for_operation(
        self,
        *,
        data_product_id: Optional[str],
        version_spec: Optional[str],
        operation: str,
        subject: str,
        status_options: Optional[Mapping[str, object]] = None,
    ) -> Any:
        product = self._load_data_product(
            data_product_id=data_product_id,
            version_spec=version_spec,
        )
        if product is None:
            return None
        identifier = data_product_id or ""
        options = self._normalise_status_options(status_options)
        self._enforce_product_status(
            product=product,
            data_product_id=identifier,
            operation=operation,
            **self._status_kwargs(options),
        )
        self._enforce_version_constraint(
            expected=version_spec,
            actual=product.version,
            data_product_id=identifier,
            subject=subject,
        )
        return product

    def _enforce_product_status(
        self,
        *,
        product: Any,
        data_product_id: str,
        operation: str,
        allowed_statuses: Optional[Iterable[str]] = None,
        allow_missing: bool = False,
        case_insensitive: bool = True,
        failure_message: Optional[str] = None,
        enforce: bool = True,
    ) -> None:
        raw_status = product.status
        version = product.version
        if raw_status is None:
            if allow_missing:
                return
            status_value = ""
        else:
            status_value = str(raw_status).strip()
            if not status_value and allow_missing:
                return

        if not status_value:
            message = (
                failure_message
                or "Data product {data_product_id}@{data_product_version} status {status!r} "
                "is not allowed for {operation} operations"
            ).format(
                data_product_id=data_product_id,
                data_product_version=version or "<unknown>",
                status=status_value,
                operation=operation,
            )
            if enforce:
                raise ValueError(message)
            return

        if allowed_statuses is None:
            statuses: Iterable[str] = ("active",)
        else:
            statuses = allowed_statuses
        prepared = [
            (str(candidate).strip().lower() if case_insensitive else str(candidate).strip())
            for candidate in statuses
            if candidate is not None and str(candidate).strip()
        ]
        comparison = status_value.lower() if case_insensitive else status_value
        if prepared and comparison in prepared:
            return
        if not prepared and allow_missing:
            return

        message = (
            failure_message
            or "Data product {data_product_id}@{data_product_version} status {status!r} "
            "is not allowed for {operation} operations"
        ).format(
            data_product_id=data_product_id,
            data_product_version=version or "<unknown>",
            status=status_value,
            operation=operation,
        )
        if enforce:
            raise ValueError(message)

    def _enforce_input_product_constraints(
        self,
        *,
        binding: DataProductInputBinding,
        port_name: str,
        product: Any,
        status_options: Optional[Mapping[str, object]] = None,
    ) -> None:
        data_product_id = binding.data_product
        options = self._normalise_status_options(status_options)
        resolved = product or self._load_data_product(
            data_product_id=data_product_id,
            version_spec=binding.data_product_version,
        )
        if resolved is not None:
            self._enforce_product_status(
                product=resolved,
                data_product_id=data_product_id,
                operation="read",
                **self._status_kwargs(options),
            )
            self._enforce_version_constraint(
                expected=binding.data_product_version,
                actual=resolved.version,
                data_product_id=data_product_id,
                subject="Data product",
            )

        source_product_id = binding.source_data_product
        if not source_product_id:
            return
        source_product = self._load_data_product(
            data_product_id=source_product_id,
            version_spec=binding.source_data_product_version,
        )
        if source_product is None:
            return
        self._enforce_product_status(
            product=source_product,
            data_product_id=source_product_id,
            operation="read",
            **self._status_kwargs(options),
        )
        self._enforce_version_constraint(
            expected=binding.source_data_product_version,
            actual=source_product.version,
            data_product_id=source_product_id,
            subject="Source data product",
        )
        source_port_name = binding.source_output_port or port_name
        port = source_product.find_output_port(source_port_name)
        if port is None:
            if binding.source_contract_version:
                raise ValueError(
                    f"Data product {source_product_id} output port "
                    f"{source_port_name} is not defined"
                )
            return
        self._enforce_version_constraint(
            expected=binding.source_contract_version,
            actual=port.version,
            data_product_id=source_product_id,
            subject="Output port contract",
        )

    def _enforce_output_product_constraints(
        self,
        *,
        binding: DataProductOutputBinding,
        port_name: str,
        product: Any,
        status_options: Optional[Mapping[str, object]] = None,
    ) -> None:
        data_product_id = binding.data_product
        options = self._normalise_status_options(status_options)
        resolved = product or self._load_data_product(
            data_product_id=data_product_id,
            version_spec=binding.data_product_version,
        )
        if resolved is None:
            return
        self._enforce_product_status(
            product=resolved,
            data_product_id=data_product_id,
            operation="write",
            **self._status_kwargs(options),
        )
        self._enforce_version_constraint(
            expected=binding.data_product_version,
            actual=resolved.version,
            data_product_id=data_product_id,
            subject="Data product",
        )

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
        lineage_event: Mapping[str, object] | None = None,
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
        summary = dict(entry)
        summary.setdefault(
            "recorded_at",
            datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        )
        context_payload = summary.get("pipeline_context")
        if isinstance(context_payload, Mapping):
            summary["pipeline_context"] = dict(context_payload)
        elif context_payload is None:
            summary["pipeline_context"] = {}
        self._store.record_pipeline_event(
            contract_id=cid,
            contract_version=cver,
            dataset_id=dataset_id,
            dataset_version=dataset_version,
            event=summary,
            lineage_event=lineage_event,
        )

__all__ = ["LocalGovernanceServiceBackend"]
