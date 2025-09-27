"""Composable dc43 components grouped by lifecycle area.

This package surfaces high-level building blocks (contract stores, drafting,
data-quality governance, runtime integrations) alongside their reference
implementations. Subpackages are structured consistently so it is easier to
swap implementations without scanning unrelated helpers.
"""

from . import contract_store, contract_drafter, data_quality
from .contract_store import ContractStore

__all__ = [
    "ContractStore",
    "contract_store",
    "contract_drafter",
    "data_quality",
]
