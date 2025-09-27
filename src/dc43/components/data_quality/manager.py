"""Data-quality manager orchestrating engine validation and governance hooks."""

from __future__ import annotations

import inspect
from dataclasses import dataclass
from typing import Any, Callable, Dict, Mapping, Optional, Sequence, Tuple, Union

from open_data_contract_standard.model import OpenDataContractStandard  # type: ignore

from dc43.components.contract_drafter import draft_from_validation_result
from dc43.components.contract_store.interface import ContractStore
from dc43.odcs import contract_identity

from .engine import ValidationResult
from .governance import DQClient, DQStatus


@dataclass
class QualityDraftContext:
    """Context forwarded when proposing a draft to governance."""

    dataset_id: Optional[str]
    dataset_version: Optional[str]
    data_format: Optional[str]
    dq_feedback: Optional[Mapping[str, object]]
    draft_context: Optional[Mapping[str, object]] = None
    pipeline_context: Optional[Mapping[str, object]] = None


@dataclass
class PipelineContext:
    """Descriptor describing the pipeline triggering a DQ interaction."""

    pipeline: Optional[str] = None
    label: Optional[str] = None
    metadata: Optional[Mapping[str, object]] = None

    def as_dict(self) -> Dict[str, object]:
        """Return the pipeline context as a dictionary."""

        payload: Dict[str, object] = {}
        if self.metadata:
            payload.update(self.metadata)
        if self.label:
            payload.setdefault("label", self.label)
        if self.pipeline:
            payload.setdefault("pipeline", self.pipeline)
        return payload


PipelineContextSpec = Union[
    PipelineContext,
    Mapping[str, object],
    Sequence[tuple[str, object]],
    str,
]


@dataclass
class QualityAssessment:
    """Outcome returned after consulting the data-quality manager."""

    status: Optional[DQStatus]
    draft: Optional[OpenDataContractStandard] = None
    observations_reused: bool = False


class DataQualityManager:
    """Coordinate validation outcomes with governance services."""

    def __init__(
        self,
        client: DQClient | None = None,
        *,
        draft_store: ContractStore | None = None,
    ):
        self._client = client
        self._draft_store = draft_store

    @property
    def client(self) -> DQClient | None:
        """Return the underlying governance client if configured."""

        return self._client

    @property
    def draft_store(self) -> ContractStore | None:
        """Return the store used to persist generated draft contracts, if any."""

        return self._draft_store

    def get_status(
        self,
        *,
        contract_id: str,
        contract_version: str,
        dataset_id: str,
        dataset_version: str,
    ) -> Optional[DQStatus]:
        """Retrieve the stored status for a dataset ↔ contract pair."""

        if not self._client:
            return None
        return self._client.get_status(
            contract_id=contract_id,
            contract_version=contract_version,
            dataset_id=dataset_id,
            dataset_version=dataset_version,
        )

    def submit_metrics(
        self,
        *,
        contract: OpenDataContractStandard,
        dataset_id: str,
        dataset_version: str,
        metrics: Mapping[str, object],
    ) -> Optional[DQStatus]:
        """Forward observation metrics to governance for evaluation."""

        if not self._client:
            return None
        return self._client.submit_metrics(
            contract=contract,
            dataset_id=dataset_id,
            dataset_version=dataset_version,
            metrics=dict(metrics),
        )

    def get_linked_contract_version(
        self,
        *,
        dataset_id: str,
        dataset_version: str | None = None,
    ) -> Optional[str]:
        """Return the contract currently associated to ``dataset_id``."""

        if not self._client:
            return None
        return self._client.get_linked_contract_version(
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
        """Record the dataset ↔ contract association when supported."""

        if not self._client:
            return
        self._client.link_dataset_contract(
            dataset_id=dataset_id,
            dataset_version=dataset_version,
            contract_id=contract_id,
            contract_version=contract_version,
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
    ) -> Optional[OpenDataContractStandard]:
        """Return a draft proposal when a validation mismatch requires it."""

        if not draft_requested:
            return None

        feedback: Mapping[str, object] | None = dq_feedback
        if feedback is None and dq_status is not None:
            payload: dict[str, object] = dict(dq_status.details or {})
            if dq_status.reason:
                payload.setdefault("reason", dq_status.reason)
            payload.setdefault("status", dq_status.status)
            feedback = payload or None

        effective_context = _build_quality_context(
            context,
            dataset_id=dataset_id,
            dataset_version=dataset_version,
            data_format=data_format,
            dq_feedback=feedback,
            pipeline_context=pipeline_context,
        )

        draft = self.propose_draft(
            validation=validation,
            base_contract=base_contract,
            bump=bump,
            context=effective_context,
            pipeline_context=effective_context.pipeline_context,
        )

        return draft

    def evaluate_dataset(
        self,
        *,
        contract: OpenDataContractStandard,
        dataset_id: str,
        dataset_version: str,
        validation: ValidationResult,
        observations: Optional[Callable[[], Tuple[Mapping[str, object], bool]]] = None,
        bump: str = "minor",
        context: QualityDraftContext | None = None,
        pipeline_context: PipelineContextSpec | None = None,
        operation: str = "read",
        draft_on_violation: bool = False,
    ) -> QualityAssessment:
        """Evaluate the dataset status and optionally return a draft proposal."""

        if not self._client:
            return QualityAssessment(status=None, draft=None, observations_reused=False)

        effective_context = _build_quality_context(
            context,
            dataset_id=dataset_id,
            dataset_version=dataset_version,
            data_format=context.data_format if context else None,
            dq_feedback=context.dq_feedback if context else None,
            pipeline_context=pipeline_context,
        )

        status = self._client.get_status(
            contract_id=contract.id,
            contract_version=contract.version,
            dataset_id=dataset_id,
            dataset_version=dataset_version,
        )

        reused = False
        if status and status.status in ("unknown", "stale") and observations is not None:
            metrics, reused = observations()
            refreshed = self._client.submit_metrics(
                contract=contract,
                dataset_id=dataset_id,
                dataset_version=dataset_version,
                metrics=dict(metrics),
            )
            if refreshed is not None:
                status = refreshed

        draft = None
        if draft_on_violation and status and status.status in ("warn", "block"):
            draft = self.review_validation_outcome(
                validation=validation,
                base_contract=contract,
                bump=bump,
                dataset_id=context.dataset_id if context else dataset_id,
                dataset_version=context.dataset_version if context else dataset_version,
                data_format=context.data_format if context else None,
                dq_status=status,
                dq_feedback=context.dq_feedback if context else None,
                draft_requested=True,
                pipeline_context=effective_context.pipeline_context,
            )

        self._record_pipeline_activity(
            contract=contract,
            dataset_id=dataset_id,
            dataset_version=dataset_version,
            operation=operation,
            pipeline_context=effective_context.pipeline_context,
            status=status,
            observations_reused=reused,
        )

        return QualityAssessment(status=status, draft=draft, observations_reused=reused)

    def propose_draft(
        self,
        *,
        validation: ValidationResult,
        base_contract: OpenDataContractStandard,
        bump: str = "minor",
        context: QualityDraftContext | None = None,
        pipeline_context: PipelineContextSpec | None = None,
    ) -> OpenDataContractStandard:
        """Generate a draft proposal for the supplied validation result."""

        base_dataset_id = context.dataset_id if context else None
        base_dataset_version = context.dataset_version if context else None
        base_data_format = context.data_format if context else None
        base_feedback = context.dq_feedback if context else None

        effective_context = _build_quality_context(
            context,
            dataset_id=base_dataset_id,
            dataset_version=base_dataset_version,
            data_format=base_data_format,
            dq_feedback=base_feedback,
            pipeline_context=pipeline_context,
        )

        draft: Optional[OpenDataContractStandard] = None

        if self._client and hasattr(self._client, "propose_draft"):
            draft = self._client.propose_draft(
                validation=validation,
                base_contract=base_contract,
                bump=bump,
                dataset_id=effective_context.dataset_id,
                dataset_version=effective_context.dataset_version,
                data_format=effective_context.data_format,
                dq_feedback=effective_context.dq_feedback,
                draft_context=effective_context.draft_context,
            )
            if draft is not None:
                if self._draft_store is not None:
                    self._draft_store.put(draft)
                return draft

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

    def get_pipeline_activity(
        self,
        *,
        dataset_id: str,
        dataset_version: Optional[str] = None,
    ) -> Sequence[Mapping[str, Any]]:
        """Return recorded pipeline activity for ``dataset_id``."""

        if not self._client:
            return []

        fetcher = getattr(self._client, "get_pipeline_activity", None)
        if not callable(fetcher):
            return []

        try:
            return fetcher(dataset_id=dataset_id, dataset_version=dataset_version) or []
        except TypeError:
            # Gracefully handle clients that expose a positional signature.
            return fetcher(dataset_id, dataset_version) or []  # type: ignore[misc]

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
        """Persist pipeline provenance for governance-aware clients."""

        if not self._client:
            return

        recorder = getattr(self._client, "record_pipeline_activity", None)
        if not callable(recorder):
            return

        contract_id: Optional[str] = None
        contract_version: Optional[str] = None
        if contract.id or contract.version:
            contract_id, contract_version = contract_identity(contract)

        payload: Dict[str, Any] = {
            "operation": operation,
            "pipeline_context": dict(pipeline_context or {}),
            "observations_reused": observations_reused,
        }
        if status is not None:
            payload["dq_status"] = status.status
            if status.reason:
                payload["dq_reason"] = status.reason
            if status.details:
                payload["dq_details"] = status.details

        try:
            recorder(
                dataset_id=dataset_id,
                dataset_version=dataset_version,
                contract_id=contract_id,
                contract_version=contract_version,
                activity=payload,
            )
        except TypeError:
            recorder(  # type: ignore[misc]
                dataset_id,
                dataset_version,
                contract_id,
                contract_version,
                payload,
            )


@dataclass
class GovernanceHandles:
    """Bundle governance dependencies so callers can share context once."""

    dq_client: DataQualityManager | DQClient | None = None
    contract_store: ContractStore | None = None

    def as_manager(self) -> DataQualityManager:
        """Return a :class:`DataQualityManager` instance configured with the bundle."""

        candidate = self.dq_client
        store = self.contract_store

        if isinstance(candidate, DataQualityManager):
            manager = candidate
            if store and manager.draft_store is not store:
                manager = DataQualityManager(manager.client, draft_store=store)
            elif store is None or manager.draft_store is store:
                return manager
            return manager

        return DataQualityManager(candidate, draft_store=store)


__all__ = [
    "DataQualityManager",
    "QualityAssessment",
    "QualityDraftContext",
    "PipelineContext",
    "GovernanceHandles",
]


def _infer_pipeline_context() -> Optional[Dict[str, str]]:
    """Return context identifying the caller requesting a contract draft."""

    frame = inspect.currentframe()
    if frame is None:
        return None

    ignored_prefixes = (
        "dc43.components.integration",
        "dc43.components.data_quality",
    )

    try:
        frame = frame.f_back
        while frame is not None:
            module_name = frame.f_globals.get("__name__", "")
            if not module_name.startswith(ignored_prefixes):
                function_name = frame.f_code.co_name
                qualname = getattr(frame.f_code, "co_qualname", function_name)
                context: Dict[str, str] = {}
                if module_name:
                    context["module"] = module_name
                if function_name:
                    context["function"] = function_name
                if qualname:
                    context.setdefault("qualname", qualname)
                if module_name and qualname:
                    context.setdefault("pipeline", f"{module_name}.{qualname}")
                elif module_name and function_name:
                    context.setdefault("pipeline", f"{module_name}.{function_name}")
                elif qualname:
                    context.setdefault("pipeline", qualname)
                filename = frame.f_code.co_filename
                if filename:
                    context.setdefault("source", filename)
                return context or None
            frame = frame.f_back
    finally:
        del frame

    return None


def _normalise_pipeline_context(
    candidate: PipelineContextSpec | Mapping[str, object] | None,
) -> Optional[Dict[str, Any]]:
    """Return a dictionary representation for ``candidate`` when possible."""

    if candidate is None:
        return None
    if isinstance(candidate, PipelineContext):
        return candidate.as_dict()
    if isinstance(candidate, str):
        value = candidate.strip()
        return {"pipeline": value} if value else None
    if isinstance(candidate, Mapping):
        return dict(candidate)
    if isinstance(candidate, Sequence):
        context: Dict[str, Any] = {}
        for item in candidate:
            if not isinstance(item, tuple) or len(item) != 2:
                continue
            key, value = item
            context[str(key)] = value
        return context or None
    return None


def _collect_pipeline_context(
    *candidates: PipelineContextSpec | Mapping[str, object] | None,
) -> Optional[Dict[str, Any]]:
    """Combine provided pipeline hints with inferred metadata."""

    explicit: Dict[str, Any] = {}
    for candidate in candidates:
        resolved = _normalise_pipeline_context(candidate)
        if resolved:
            explicit.update(resolved)

    inferred = _infer_pipeline_context()
    context: Dict[str, Any] = {}
    if inferred:
        context.update(inferred)
    if explicit:
        context.update(explicit)
    return context or None


def _merge_draft_context(
    base: Optional[Mapping[str, Any]],
    *,
    dataset_id: Optional[str],
    dataset_version: Optional[str],
    data_format: Optional[str],
    pipeline_context: Optional[Mapping[str, Any]],
) -> Optional[Dict[str, Any]]:
    """Combine the supplied draft context with inferred metadata."""

    context: Dict[str, Any] = {}
    if pipeline_context:
        context.update(pipeline_context)
    if base:
        context.update(base)

    if dataset_id and "dataset_id" not in context:
        context["dataset_id"] = dataset_id
    if dataset_version and "dataset_version" not in context:
        context["dataset_version"] = dataset_version
    if data_format and "data_format" not in context:
        context["data_format"] = data_format

    return context or None


def _build_quality_context(
    context: QualityDraftContext | None,
    *,
    dataset_id: Optional[str],
    dataset_version: Optional[str],
    data_format: Optional[str],
    dq_feedback: Optional[Mapping[str, object]],
    pipeline_context: Optional[PipelineContextSpec] = None,
) -> QualityDraftContext:
    """Return a :class:`QualityDraftContext` populated with inferred metadata."""

    resolved_dataset_id = context.dataset_id if context and context.dataset_id else dataset_id
    resolved_dataset_version = (
        context.dataset_version if context and context.dataset_version else dataset_version
    )
    resolved_data_format = context.data_format if context and context.data_format else data_format
    resolved_feedback = context.dq_feedback if context and context.dq_feedback else dq_feedback

    pipeline_metadata = _collect_pipeline_context(
        context.pipeline_context if context else None,
        pipeline_context,
    )

    merged_context = _merge_draft_context(
        context.draft_context if context else None,
        dataset_id=resolved_dataset_id,
        dataset_version=resolved_dataset_version,
        data_format=resolved_data_format,
        pipeline_context=pipeline_metadata,
    )

    return QualityDraftContext(
        dataset_id=resolved_dataset_id,
        dataset_version=resolved_dataset_version,
        data_format=resolved_data_format,
        dq_feedback=resolved_feedback,
        draft_context=merged_context,
        pipeline_context=pipeline_metadata,
    )
