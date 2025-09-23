"""Compatibility shim importing the Collibra-backed contract store."""

from dc43.components.contract_store.impl.collibra import CollibraContractStore

__all__ = ["CollibraContractStore"]
