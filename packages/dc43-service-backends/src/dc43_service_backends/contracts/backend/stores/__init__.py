"""Reference contract store implementations."""

from .filesystem import FSContractStore
from .delta import DeltaContractStore

try:  # pragma: no cover - optional dependency
    from .sql import SQLContractStore
except ModuleNotFoundError:  # pragma: no cover
    SQLContractStore = None  # type: ignore
from .collibra import (
    CollibraContractAdapter,
    CollibraContractGateway,
    CollibraContractStore,
    ContractSummary,
    HttpCollibraContractAdapter,
    HttpCollibraContractGateway,
    StubCollibraContractAdapter,
    StubCollibraContractGateway,
)

__all__ = [
    "CollibraContractAdapter",
    "CollibraContractGateway",
    "CollibraContractStore",
    "ContractSummary",
    "DeltaContractStore",
    "FSContractStore",
    "HttpCollibraContractAdapter",
    "HttpCollibraContractGateway",
    "StubCollibraContractAdapter",
    "StubCollibraContractGateway",
]

if SQLContractStore is not None:
    __all__.append("SQLContractStore")
