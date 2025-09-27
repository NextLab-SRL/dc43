"""Local in-process implementation of :mod:`dc43.services.contracts`."""

from __future__ import annotations

from typing import Optional

from open_data_contract_standard.model import OpenDataContractStandard  # type: ignore

from dc43.components.contract_store.interface import ContractStore

from .client import ContractServiceClient


class LocalContractServiceClient(ContractServiceClient):
    """Adapter that fulfils the service contract using a :class:`ContractStore`."""

    def __init__(self, store: ContractStore) -> None:
        self._store = store

    def get(self, contract_id: str, contract_version: str) -> OpenDataContractStandard:
        return self._store.get(contract_id, contract_version)

    def link_dataset_contract(
        self,
        *,
        dataset_id: str,
        dataset_version: str,
        contract_id: str,
        contract_version: str,
    ) -> None:
        # Local stub does not persist linkage but keeps API surface intact.
        self._store.put(self._store.get(contract_id, contract_version))

    def get_linked_contract_version(
        self,
        *,
        dataset_id: str,
        dataset_version: Optional[str] = None,
    ) -> Optional[str]:
        # No linkage is tracked locally, return ``None`` to signal absence.
        return None


__all__ = ["LocalContractServiceClient"]
