from __future__ import annotations

"""Abstract interface for contract storage backends."""

from abc import ABC, abstractmethod
from typing import List, Optional, Any, Dict
from open_data_contract_standard.model import OpenDataContractStandard  # type: ignore


class ContractStore(ABC):
    """Interface for reading/writing ODCS contracts in a registry."""
    @abstractmethod
    def put(self, contract: OpenDataContractStandard) -> None:
        """Store or update a contract (ODCS dict/object)."""
        ...

    @abstractmethod
    def get(self, contract_id: str, version: str) -> OpenDataContractStandard:
        """Fetch a contract by identity, returning an ODCS model."""
        ...

    @abstractmethod
    def list_versions(self, contract_id: str) -> List[str]:
        """List available versions for a given contract id."""
        ...

    def latest(self, contract_id: str) -> Optional[OpenDataContractStandard]:
        """Return the latest version for the contract id, if any."""
        versions = self.list_versions(contract_id)
        if not versions:
            return None
        versions.sort(key=lambda v: tuple(int(x) for x in v.split(".")[:3]))
        return self.get(contract_id, versions[-1])
