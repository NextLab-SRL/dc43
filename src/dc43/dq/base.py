from __future__ import annotations

"""Data Quality client interface.

Abstraction for an external DQ/DO service that can tell whether a given
dataset@version satisfies the contract@version and, when needed, request
metrics to be computed and submitted.
"""

from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Protocol
from open_data_contract_standard.model import OpenDataContractStandard  # type: ignore


@dataclass
class DQStatus:
    """Status returned by the DQ service.

    - ``ok``: no blocking issues
    - ``warn``: non-blocking issues
    - ``block``: blocking issues; callers may raise on this when enforcing
    - ``unknown``: dataset version not yet evaluated
    """
    status: str  # one of: ok, warn, block, unknown
    reason: Optional[str] = None
    details: Dict[str, Any] = None


class DQClient(Protocol):
    def expected_metrics(self, contract: OpenDataContractStandard) -> Dict[str, Any]:
        """Return a metric spec the client needs to evaluate quality.

        Example shape suggested:
        {
          "row_count": true,
          "fields": {
            "amount": ["null_count", "min", "max"],
            "currency": ["null_count", {"enum_violations": ["EUR", "USD"]}],
          }
        }
        """

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
