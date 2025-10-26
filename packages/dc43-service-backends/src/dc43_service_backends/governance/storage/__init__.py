"""Persistence adapters for governance metadata and validation artefacts."""

from .interface import GovernanceStore
from .memory import InMemoryGovernanceStore
from .filesystem import FilesystemGovernanceStore

try:  # pragma: no cover - optional dependency
    from .sql import SQLGovernanceStore
except ModuleNotFoundError:  # pragma: no cover - sqlalchemy optional
    SQLGovernanceStore = None  # type: ignore[assignment]

try:  # pragma: no cover - optional dependencies
    from .delta import DeltaGovernanceStore
except ModuleNotFoundError:  # pragma: no cover - pyspark optional
    DeltaGovernanceStore = None  # type: ignore[assignment]

try:  # pragma: no cover - optional dependencies
    from .http import HttpGovernanceStore
except ModuleNotFoundError:  # pragma: no cover - httpx optional
    HttpGovernanceStore = None  # type: ignore[assignment]

__all__ = [
    "GovernanceStore",
    "InMemoryGovernanceStore",
    "FilesystemGovernanceStore",
    "SQLGovernanceStore",
    "DeltaGovernanceStore",
    "HttpGovernanceStore",
]
