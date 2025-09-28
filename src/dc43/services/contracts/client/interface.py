"""Client protocols for interacting with contract services."""

from __future__ import annotations

from typing import Optional, Protocol, Sequence

from open_data_contract_standard.model import OpenDataContractStandard  # type: ignore


class ContractServiceClient(Protocol):
    """Actions exposed by a contract management service."""

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


__all__ = ["ContractServiceClient"]
