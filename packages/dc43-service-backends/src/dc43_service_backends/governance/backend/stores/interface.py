"""Interface for persisting governance artefacts."""

from __future__ import annotations

from typing import Mapping, Optional, Protocol, Sequence

from dc43_service_clients.data_quality import ValidationResult


class GovernanceStore(Protocol):
    """Persistence contract used by :class:`LocalGovernanceServiceBackend`."""

    def save_status(
        self,
        *,
        contract_id: str,
        contract_version: str,
        dataset_id: str,
        dataset_version: str,
        status: ValidationResult | None,
    ) -> None:
        """Persist the latest validation ``status`` for the dataset version."""

    def load_status(
        self,
        *,
        contract_id: str,
        contract_version: str,
        dataset_id: str,
        dataset_version: str,
    ) -> ValidationResult | None:
        """Return the stored validation status for the dataset version."""

    def record_pipeline_event(
        self,
        *,
        contract_id: str,
        contract_version: str,
        dataset_id: str,
        dataset_version: str,
        event: Mapping[str, object],
        lineage_event: Mapping[str, object] | None = None,
    ) -> None:
        """Append ``event`` metadata to the pipeline activity log."""

    def list_datasets(self) -> Sequence[str]:
        """Return dataset identifiers recorded by the governance store."""

    def load_pipeline_activity(
        self,
        *,
        dataset_id: str,
        dataset_version: Optional[str] = None,
    ) -> Sequence[Mapping[str, object]]:
        """Return pipeline activity entries for the dataset."""

    def link_dataset_contract(
        self,
        *,
        dataset_id: str,
        dataset_version: str,
        contract_id: str,
        contract_version: str,
    ) -> None:
        """Persist an association between the dataset version and contract."""

    def get_linked_contract_version(
        self,
        *,
        dataset_id: str,
        dataset_version: Optional[str] = None,
    ) -> str | None:
        """Return the contract reference linked to the dataset if any."""

    def load_metrics(
        self,
        *,
        dataset_id: str,
        dataset_version: Optional[str] = None,
        contract_id: Optional[str] = None,
        contract_version: Optional[str] = None,
    ) -> Sequence[Mapping[str, object]]:
        """Return stored metric entries associated with the dataset."""

    def load_status_matrix_entries(
        self,
        *,
        dataset_id: str,
        dataset_versions: Sequence[str] | None = None,
        contract_ids: Sequence[str] | None = None,
    ) -> Sequence[Mapping[str, object]]:
        """Return persisted status rows for the dataset filtered by version and contract."""


__all__ = ["GovernanceStore"]
