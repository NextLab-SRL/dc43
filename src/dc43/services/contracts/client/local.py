"""In-process client implementation for contract services."""

from __future__ import annotations

from typing import Optional, Sequence

from open_data_contract_standard.model import OpenDataContractStandard  # type: ignore

from dc43.services.contracts.backend.stores.interface import ContractStore

from ..backend import ContractServiceBackend, LocalContractServiceBackend
from .interface import ContractServiceClient


class LocalContractServiceClient(ContractServiceClient):
    """Adapter that fulfils the client contract via a backend instance."""

    def __init__(
        self,
        backend: ContractServiceBackend | ContractStore | None = None,
        *,
        store: ContractStore | None = None,
    ) -> None:
        if isinstance(backend, ContractStore):
            store = backend
            backend = None
        if backend is None:
            if store is None:
                raise ValueError("a ContractStore is required for the local backend")
            backend = LocalContractServiceBackend(store)
        self._backend = backend
    def get(self, contract_id: str, contract_version: str) -> OpenDataContractStandard:
        return self._backend.get(contract_id, contract_version)

    def latest(self, contract_id: str) -> Optional[OpenDataContractStandard]:
        return self._backend.latest(contract_id)

    def list_versions(self, contract_id: str) -> Sequence[str]:
        return self._backend.list_versions(contract_id)

    def link_dataset_contract(
        self,
        *,
        dataset_id: str,
        dataset_version: str,
        contract_id: str,
        contract_version: str,
    ) -> None:
        self._backend.link_dataset_contract(
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
        return self._backend.get_linked_contract_version(
            dataset_id=dataset_id,
            dataset_version=dataset_version,
        )


__all__ = ["LocalContractServiceClient"]
