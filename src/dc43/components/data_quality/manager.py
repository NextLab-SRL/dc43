"""Data-quality manager orchestrating engine validation and governance hooks."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Callable, Mapping, Optional, Tuple

from open_data_contract_standard.model import OpenDataContractStandard  # type: ignore

from dc43.components.contract_drafter import draft_from_validation_result

from .engine import ValidationResult
from .governance import DQClient, DQStatus


@dataclass
class QualityDraftContext:
    """Context forwarded when proposing a draft to governance."""

    dataset_id: Optional[str]
    dataset_version: Optional[str]
    data_format: Optional[str]
    dq_feedback: Optional[Mapping[str, object]]


@dataclass
class QualityAssessment:
    """Outcome returned after consulting the data-quality manager."""

    status: Optional[DQStatus]
    draft: Optional[OpenDataContractStandard] = None
    observations_reused: bool = False


class DataQualityManager:
    """Coordinate validation outcomes with governance services."""

    def __init__(self, client: DQClient | None = None):
        self._client = client

    @property
    def client(self) -> DQClient | None:
        """Return the underlying governance client if configured."""

        return self._client

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

    def get_linked_contract_version(self, *, dataset_id: str) -> Optional[str]:
        """Return the contract currently associated to ``dataset_id``."""

        if not self._client:
            return None
        return self._client.get_linked_contract_version(dataset_id=dataset_id)

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
        context: QualityDraftContext | None = None,
        draft_requested: bool = False,
    ) -> Optional[OpenDataContractStandard]:
        """Return a draft proposal when a validation mismatch requires it."""

        if not draft_requested:
            return None

        dataset_id = context.dataset_id if context else None
        dataset_version = context.dataset_version if context else None
        data_format = context.data_format if context else None
        dq_feedback = context.dq_feedback if context else None

        return draft_from_validation_result(
            validation=validation,
            base_contract=base_contract,
            bump=bump,
            dataset_id=dataset_id,
            dataset_version=dataset_version,
            data_format=data_format,
            dq_feedback=dq_feedback,
        )

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
        draft_on_violation: bool = False,
    ) -> QualityAssessment:
        """Evaluate the dataset status and optionally return a draft proposal."""

        if not self._client:
            return QualityAssessment(status=None, draft=None, observations_reused=False)

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
            feedback: Mapping[str, object] | None = None
            if context and context.dq_feedback:
                feedback = dict(context.dq_feedback)
            else:
                feedback = {}
            feedback = dict(feedback or {})
            feedback.setdefault("status", status.status)
            if status.reason:
                feedback.setdefault("reason", status.reason)
            local_context = QualityDraftContext(
                dataset_id=context.dataset_id if context else None,
                dataset_version=context.dataset_version if context else None,
                data_format=context.data_format if context else None,
                dq_feedback=feedback or None,
            )
            draft = self.propose_draft(
                validation=validation,
                base_contract=contract,
                bump=bump,
                context=local_context,
            )

        return QualityAssessment(status=status, draft=draft, observations_reused=reused)

    def propose_draft(
        self,
        *,
        validation: ValidationResult,
        base_contract: OpenDataContractStandard,
        bump: str = "minor",
        context: QualityDraftContext | None = None,
    ) -> OpenDataContractStandard:
        """Generate a draft proposal for the supplied validation result."""

        dataset_id = context.dataset_id if context else None
        dataset_version = context.dataset_version if context else None
        data_format = context.data_format if context else None
        dq_feedback = context.dq_feedback if context else None

        if self._client and hasattr(self._client, "propose_draft"):
            draft = self._client.propose_draft(
                validation=validation,
                base_contract=base_contract,
                bump=bump,
                dataset_id=dataset_id,
                dataset_version=dataset_version,
                data_format=data_format,
                dq_feedback=dq_feedback,
            )
            if draft is not None:
                return draft

        return draft_from_validation_result(
            validation=validation,
            base_contract=base_contract,
            bump=bump,
            dataset_id=dataset_id,
            dataset_version=dataset_version,
            data_format=data_format,
            dq_feedback=dq_feedback,
        )


__all__ = ["DataQualityManager", "QualityAssessment", "QualityDraftContext"]
