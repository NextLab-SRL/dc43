"""Protocol for remote contract management services."""

from __future__ import annotations

from typing import Optional, Protocol

from open_data_contract_standard.model import OpenDataContractStandard  # type: ignore


class ContractServiceClient(Protocol):
    """Actions exposed by a contract management service."""

    def get(self, contract_id: str, contract_version: str) -> OpenDataContractStandard:
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
