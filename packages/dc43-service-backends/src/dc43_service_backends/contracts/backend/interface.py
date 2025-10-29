"""Interfaces for implementing contract management backends."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Optional, Protocol, Sequence

from open_data_contract_standard.model import OpenDataContractStandard  # type: ignore


@dataclass(slots=True)
class ContractListing:
    """Paginated contract identifiers returned by service backends."""

    items: Sequence[str]
    total: int
    limit: int | None = None
    offset: int = 0


class ContractServiceBackend(Protocol):
    """Operations exposed by a contract management service runtime."""

    def put(self, contract: OpenDataContractStandard) -> None:
        """Persist ``contract`` making it available for downstream consumers."""

    def list_contracts(
        self, *, limit: int | None = None, offset: int = 0
    ) -> ContractListing:
        """Return contract identifiers visible to the backend."""

    def get(self, contract_id: str, contract_version: str) -> OpenDataContractStandard:
        ...

    def latest(self, contract_id: str) -> Optional[OpenDataContractStandard]:
        ...

    def list_versions(self, contract_id: str) -> Sequence[str]:
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

    def get_linked_contract_version(
        self,
        *,
        dataset_id: str,
        dataset_version: Optional[str] = None,
    ) -> Optional[str]:
        ...


__all__ = ["ContractListing", "ContractServiceBackend"]
