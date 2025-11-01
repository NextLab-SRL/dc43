"""Local stub of the contract service backend."""

from __future__ import annotations

from typing import Optional, Sequence

from open_data_contract_standard.model import OpenDataContractStandard  # type: ignore

from .stores.interface import ContractStore

from .interface import ContractListing, ContractServiceBackend


class LocalContractServiceBackend(ContractServiceBackend):
    """Backend backed by an in-memory :class:`ContractStore`."""

    def __init__(self, store: ContractStore) -> None:
        self._store = store

    def put(self, contract: OpenDataContractStandard) -> None:  # noqa: D401 - short docstring
        self._store.put(contract)

    def list_contracts(
        self, *, limit: int | None = None, offset: int = 0
    ) -> ContractListing:  # noqa: D401
        contract_ids = sorted(self._store.list_contracts())
        total = len(contract_ids)
        start = max(int(offset), 0)
        end = total
        if limit is not None:
            span = max(int(limit), 0)
            end = min(start + span, total)
        return ContractListing(
            items=contract_ids[start:end],
            total=total,
            limit=limit,
            offset=start,
        )

    def get(self, contract_id: str, contract_version: str) -> OpenDataContractStandard:
        return self._store.get(contract_id, contract_version)

    def latest(self, contract_id: str) -> Optional[OpenDataContractStandard]:
        return self._store.latest(contract_id)

    def list_versions(self, contract_id: str) -> Sequence[str]:
        return self._store.list_versions(contract_id)

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


__all__ = ["LocalContractServiceBackend"]
