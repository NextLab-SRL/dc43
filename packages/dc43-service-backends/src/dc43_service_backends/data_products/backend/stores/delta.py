"""Delta Lake backed persistence for ODPS documents."""

from __future__ import annotations

import logging
from typing import Iterable, Optional, Sequence

try:  # pragma: no cover - optional dependency
    from pyspark.sql import SparkSession
except Exception:  # pragma: no cover - Spark is optional for most deployments
    SparkSession = object  # type: ignore

from dc43_service_clients.odps import OpenDataProductStandard, as_odps_dict, to_model

from .._shared import _version_key
from .interface import DataProductStore


logger = logging.getLogger(__name__)


class DeltaDataProductStore(DataProductStore):
    """Persist ODPS documents in a Delta table or Unity Catalog object."""

    def __init__(
        self,
        spark: SparkSession,
        *,
        table: str | None = None,
        path: str | None = None,
        log_sql: bool = False,
    ) -> None:
        if not (table or path):
            raise ValueError("Provide either a Unity Catalog table name or a Delta path")
        self._spark = spark
        self._table = table
        self._path = path
        self._log_sql = log_sql
        self._ensure_table()

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------
    def _table_ref(self) -> str:
        return self._table if self._table else f"delta.`{self._path}`"

    def _execute_sql(self, statement: str):  # pragma: no cover - logging shim
        if self._log_sql:
            logger.info("Spark SQL (data products): %s", statement.strip())
        return self._spark.sql(statement)

    def _ensure_table(self) -> None:
        ref = self._table_ref()
        if self._table:
            self._execute_sql(
                f"""
                CREATE TABLE IF NOT EXISTS {ref} (
                    data_product_id STRING,
                    version STRING,
                    status STRING,
                    json STRING,
                    updated_at TIMESTAMP
                ) USING DELTA
                PARTITIONED BY (data_product_id)
                TBLPROPERTIES (delta.autoOptimize.autoCompact = true)
                """
            )
        else:
            self._execute_sql(
                f"""
                CREATE TABLE IF NOT EXISTS {ref} (
                    data_product_id STRING,
                    version STRING,
                    status STRING,
                    json STRING,
                    updated_at TIMESTAMP
                ) USING DELTA
                LOCATION '{self._path}'
                PARTITIONED BY (data_product_id)
                TBLPROPERTIES (delta.autoOptimize.autoCompact = true)
                """
            )

    def _merge_row(self, product: OpenDataProductStandard) -> None:
        import json

        ref = self._table_ref()
        payload = as_odps_dict(product)
        json_payload = json.dumps(payload, separators=(",", ":"))
        status = product.status or "draft"
        json_sql = json_payload.replace("'", "''")
        status_sql = status.replace("'", "''")
        self._execute_sql(
            f"""
            MERGE INTO {ref} t
            USING (SELECT
                    '{product.id}' as data_product_id,
                    '{product.version}' as version,
                    '{status_sql}' as status,
                    '{json_sql}' as json,
                    current_timestamp() as updated_at) s
            ON t.data_product_id = s.data_product_id AND t.version = s.version
            WHEN MATCHED THEN UPDATE SET status = s.status, json = s.json, updated_at = s.updated_at
            WHEN NOT MATCHED THEN INSERT *
            """
        )

    @staticmethod
    def _safe_json(payload: str) -> dict[str, object]:
        import json

        return json.loads(payload)

    def _collect_versions(self, data_product_id: str) -> Iterable[tuple[str, object]]:
        ref = self._table_ref()
        rows = self._execute_sql(
            f"SELECT version, json FROM {ref} WHERE data_product_id = '{data_product_id}'"
        ).collect()
        for row in rows:
            if not row:
                continue
            version = str(row[0]).strip()
            if not version:
                continue
            yield version, row[1]

    # ------------------------------------------------------------------
    # DataProductStore implementation
    # ------------------------------------------------------------------
    def put(self, product: OpenDataProductStandard) -> None:  # noqa: D401 - short docstring
        if not product.version:
            raise ValueError("Data product version is required")
        self._merge_row(product)

    def get(self, data_product_id: str, version: str) -> OpenDataProductStandard:  # noqa: D401
        ref = self._table_ref()
        rows = self._execute_sql(
            f"SELECT json FROM {ref} WHERE data_product_id = '{data_product_id}' AND version = '{version}'"
        ).head(1)
        if not rows:
            raise FileNotFoundError(f"data product {data_product_id}:{version} not found")
        return to_model(self._safe_json(rows[0][0]))

    def latest(self, data_product_id: str) -> Optional[OpenDataProductStandard]:  # noqa: D401
        entries = list(self._collect_versions(data_product_id))
        if not entries:
            return None
        latest_row = max(entries, key=lambda row: _version_key(row[0]))
        return to_model(self._safe_json(latest_row[1]))

    def list_versions(self, data_product_id: str) -> Sequence[str]:  # noqa: D401
        versions = sorted(
            (version for version, _ in self._collect_versions(data_product_id)),
            key=_version_key,
        )
        return [version for version in versions if version]

    def list_data_product_ids(self) -> Sequence[str]:  # noqa: D401
        ref = self._table_ref()
        rows = self._execute_sql(f"SELECT DISTINCT data_product_id FROM {ref}").collect()
        product_ids = {str(row[0]) for row in rows if row and row[0]}
        return sorted(product_ids)


__all__ = ["DeltaDataProductStore"]
