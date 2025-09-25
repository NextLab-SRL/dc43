"""Protocols for data-quality governance clients."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Mapping, Optional, Protocol

from open_data_contract_standard.model import OpenDataContractStandard  # type: ignore

from dc43.components.data_quality.engine import ValidationResult


@dataclass
class DQStatus:
    """Status returned by a DQ governance service."""

    status: str  # one of: ok, warn, block, unknown
    reason: Optional[str] = None
    details: Dict[str, Any] = None


class DQClient(Protocol):
    """Interface expected by dc43 when coordinating quality verdicts."""

    def get_status(
        self,
        *,
        contract_id: str,
        contract_version: str,
        dataset_id: str,
        dataset_version: str,
    ) -> DQStatus:
        ...

    def submit_metrics(
        self,
        *,
        contract: OpenDataContractStandard,
        dataset_id: str,
        dataset_version: str,
        metrics: Dict[str, Any],
    ) -> DQStatus:
        ...

    def propose_draft(
        self,
        *,
        validation: ValidationResult,
        base_contract: OpenDataContractStandard,
        bump: str = "minor",
        dataset_id: Optional[str] = None,
        dataset_version: Optional[str] = None,
        data_format: Optional[str] = None,
        dq_feedback: Optional[Mapping[str, Any]] = None,
    ) -> Optional[OpenDataContractStandard]:
        """Return a draft contract proposal based on a validation outcome."""
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

    def get_linked_contract_version(self, *, dataset_id: str) -> Optional[str]:
        """Return contract version associated to dataset if tracked (format: "<contract_id>:<version>")."""
        ...


__all__ = ["DQClient", "DQStatus"]
