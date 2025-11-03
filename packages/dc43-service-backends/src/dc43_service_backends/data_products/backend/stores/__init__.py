"""Persistence adapters for data product definitions."""

from .interface import DataProductStore
from .memory import InMemoryDataProductStore
from .filesystem import FilesystemDataProductStore

try:  # pragma: no cover - optional dependency
    from .sql import SQLDataProductStore  # type: ignore
except ModuleNotFoundError:  # pragma: no cover - sqlalchemy optional
    SQLDataProductStore = None  # type: ignore[assignment]

try:  # pragma: no cover - optional dependency
    from .delta import DeltaDataProductStore  # type: ignore
except ModuleNotFoundError:  # pragma: no cover - pyspark optional
    DeltaDataProductStore = None  # type: ignore[assignment]

__all__ = [
    "DataProductStore",
    "InMemoryDataProductStore",
    "FilesystemDataProductStore",
    "SQLDataProductStore",
    "DeltaDataProductStore",
]
