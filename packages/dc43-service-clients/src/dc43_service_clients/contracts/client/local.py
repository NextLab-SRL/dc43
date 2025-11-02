"""Local contract client delegating to backend implementations."""

from __future__ import annotations

from typing import Mapping, Optional, Sequence, TYPE_CHECKING

from open_data_contract_standard.model import OpenDataContractStandard  # type: ignore

from .interface import ContractServiceClient

if TYPE_CHECKING:  # pragma: no cover - imported for type checking only
    from dc43_service_backends.contracts import (
        ContractServiceBackend,
        ContractStore,
    )
else:  # pragma: no cover - help type checkers resolve names
    ContractServiceBackend = ContractStore = object  # type: ignore


class LocalContractServiceClient(ContractServiceClient):
    """Adapter that fulfils the client contract via a backend instance."""

    def __init__(
        self,
        backend: "ContractServiceBackend | ContractStore | None" = None,
        *,
        store: "ContractStore | None" = None,
    ) -> None:
        from dc43_service_backends.contracts import (
            ContractStore as _ContractStore,
            LocalContractServiceBackend as _LocalContractServiceBackend,
        )

        if isinstance(backend, _ContractStore):
            store = backend
            backend = None
        if backend is None:
            if store is None:
                raise ValueError("a ContractStore is required for the local backend")
            backend = _LocalContractServiceBackend(store)
        self._backend = backend

    def put(self, contract: OpenDataContractStandard) -> None:
        self._backend.put(contract)

    def get(self, contract_id: str, contract_version: str) -> OpenDataContractStandard:
        return self._backend.get(contract_id, contract_version)

    def latest(self, contract_id: str) -> Optional[OpenDataContractStandard]:
        return self._backend.latest(contract_id)

    def list_versions(self, contract_id: str) -> Sequence[str]:
        return self._backend.list_versions(contract_id)

    def list_contracts(
        self, *, limit: int | None = None, offset: int = 0
    ) -> Mapping[str, object]:
        listing = self._backend.list_contracts(limit=limit, offset=offset)
        return {
            "items": [str(item) for item in listing.items],
            "total": int(listing.total),
            "limit": listing.limit,
            "offset": listing.offset,
        }

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
