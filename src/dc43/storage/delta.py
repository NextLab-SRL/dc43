"""Compatibility shim importing the Delta-backed contract store."""

from dc43.components.contract_store.impl.delta import DeltaContractStore

__all__ = ["DeltaContractStore"]
