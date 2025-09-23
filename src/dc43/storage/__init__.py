"""Backward-compatibility shims for contract store imports."""

from dc43.components.contract_store import ContractStore, FSContractStore

__all__ = ["ContractStore", "FSContractStore"]
