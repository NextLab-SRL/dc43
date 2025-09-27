"""In-process helpers for exercising the governance service in tests/demo."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, Optional

from open_data_contract_standard.model import OpenDataContractStandard  # type: ignore

from dc43.components.contract_store.interface import ContractStore
from dc43.components.data_quality.manager import DataQualityManager

from .service import ContractManagerClient, GovernanceServiceClient


@dataclass
class LocalContractManager(ContractManagerClient):
    """Stub contract manager backed by a local :class:`ContractStore`."""

    store: ContractStore

    def __post_init__(self) -> None:
        self._links: Dict[tuple[str, Optional[str]], tuple[str, str]] = {}

    def get(self, contract_id: str, contract_version: str) -> OpenDataContractStandard:
        return self.store.get(contract_id, contract_version)

    def link_dataset_contract(
        self,
        *,
        dataset_id: str,
        dataset_version: str,
        contract_id: str,
        contract_version: str,
    ) -> None:
        self._links[(dataset_id, dataset_version)] = (contract_id, contract_version)

    def get_linked_contract_version(
        self,
        *,
        dataset_id: str,
        dataset_version: Optional[str] = None,
    ) -> Optional[str]:
        if dataset_version is not None:
            entry = self._links.get((dataset_id, dataset_version))
            if entry:
                return f"{entry[0]}:{entry[1]}"
        # Fall back to version-agnostic association when available.
        entry = self._links.get((dataset_id, None))
        if entry:
            return f"{entry[0]}:{entry[1]}"
        return None


def build_local_governance_service(
    store: ContractStore,
    *,
    dq_manager: DataQualityManager | None = None,
) -> GovernanceServiceClient:
    """Return a governance service using in-memory contract and DQ managers."""

    manager = dq_manager or DataQualityManager()
    contract_client = LocalContractManager(store)
    return GovernanceServiceClient(
        contract_client=contract_client,
        dq_manager=manager,
        draft_store=store,
    )


__all__ = ["LocalContractManager", "build_local_governance_service"]
