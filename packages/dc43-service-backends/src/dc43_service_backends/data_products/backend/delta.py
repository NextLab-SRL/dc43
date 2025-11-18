"""Delta-backed data product backend for Unity Catalog deployments."""

from __future__ import annotations

try:  # pragma: no cover - optional dependency
    from pyspark.sql import SparkSession
except Exception:  # pragma: no cover - Spark is optional for most deployments
    SparkSession = object  # type: ignore

from .local import _StoreBackedDataProductServiceBackend
from .stores import DataProductStore

try:  # pragma: no cover - optional dependency
    from .stores.delta import DeltaDataProductStore
except ModuleNotFoundError:  # pragma: no cover - pyspark optional
    DeltaDataProductStore = None  # type: ignore[assignment]


class DeltaDataProductServiceBackend(_StoreBackedDataProductServiceBackend):
    """Persist ODPS documents in a Delta table or Unity Catalog object."""

    def __init__(
        self,
        spark: SparkSession,
        *,
        table: str | None = None,
        path: str | None = None,
        store: DataProductStore | None = None,
        log_sql: bool = False,
    ) -> None:
        if store is None:
            if DeltaDataProductStore is None:
                raise RuntimeError(
                    "pyspark is required when using the Delta data product backend"
                )
            store = DeltaDataProductStore(spark, table=table, path=path, log_sql=log_sql)
        super().__init__(store)


__all__ = ["DeltaDataProductServiceBackend"]
