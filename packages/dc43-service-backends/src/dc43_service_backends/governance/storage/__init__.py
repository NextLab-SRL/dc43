"""Persistence adapters for governance metadata and validation artefacts."""

from typing import TYPE_CHECKING, Any

from .interface import GovernanceStore
from .memory import InMemoryGovernanceStore
from .filesystem import FilesystemGovernanceStore

if TYPE_CHECKING:  # pragma: no cover - mypy/type checkers only
    from .sql import SQLGovernanceStore as _SQLGovernanceStore

try:
    from .sql import SQLGovernanceStore as _SQLGovernanceStore
except ModuleNotFoundError as exc:
    if exc.name != "sqlalchemy":
        raise

    class SQLGovernanceStore:  # type: ignore[no-redef]
        """Placeholder raising a clearer error when SQLAlchemy is missing."""

        def __init__(self, *args: Any, **kwargs: Any) -> None:
            raise ModuleNotFoundError(
                "sqlalchemy is required for SQLGovernanceStore. "
                "Install dc43-service-backends[sql] or add sqlalchemy>=2.0."
            ) from exc

        def __getattr__(self, name: str) -> Any:
            raise ModuleNotFoundError(
                "sqlalchemy is required for SQLGovernanceStore. "
                "Install dc43-service-backends[sql] or add sqlalchemy>=2.0."
            ) from exc

else:  # pragma: no cover - exercised in integration tests
    SQLGovernanceStore = _SQLGovernanceStore

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
