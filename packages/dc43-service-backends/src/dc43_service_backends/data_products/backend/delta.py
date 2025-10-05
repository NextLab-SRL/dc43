"""Delta-backed data product backend for Unity Catalog deployments."""

from __future__ import annotations

from typing import Iterable, Optional

try:  # pragma: no cover - optional dependency
    from pyspark.sql import SparkSession
except Exception:  # pragma: no cover - Spark is optional for most deployments
    SparkSession = object  # type: ignore

from .local import LocalDataProductServiceBackend
from dc43_service_clients.odps import OpenDataProductStandard, to_model, as_odps_dict


class DeltaDataProductServiceBackend(LocalDataProductServiceBackend):
    """Persist ODPS documents in a Delta table or Unity Catalog object."""

    def __init__(
        self,
        spark: SparkSession,
        *,
        table: str | None = None,
        path: str | None = None,
    ) -> None:
        if not (table or path):
            raise ValueError("Provide either a Unity Catalog table name or a Delta path")
        self._spark = spark
        self._table = table
        self._path = path
        self._ensure_table()
        super().__init__()
        self._load_existing()

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------
    def _table_ref(self) -> str:
        return self._table if self._table else f"delta.`{self._path}`"

    def _ensure_table(self) -> None:
        ref = self._table_ref()
        if self._table:
            self._spark.sql(
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
            self._spark.sql(
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

    def _load_existing(self) -> None:
        ref = self._table_ref()
        rows = self._spark.sql(f"SELECT data_product_id, version, json FROM {ref}").collect()
        for row in rows:
            payload = to_model(self._safe_json(row[2]))
            LocalDataProductServiceBackend.put(self, payload)

    # ------------------------------------------------------------------
    # Persistence helpers
    # ------------------------------------------------------------------
    def _merge_row(self, product: OpenDataProductStandard) -> None:
        import json

        ref = self._table_ref()
        payload = as_odps_dict(product)
        json_payload = json.dumps(payload, separators=(",", ":"))
        status = product.status or "draft"
        json_sql = json_payload.replace("'", "''")
        status_sql = status.replace("'", "''")
        self._spark.sql(
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

    # ------------------------------------------------------------------
    # Overrides
    # ------------------------------------------------------------------
    def put(self, product: OpenDataProductStandard) -> None:  # noqa: D401
        super().put(product)
        if not product.version:
            return
        self._merge_row(product)

    def _existing_versions(self, data_product_id: str) -> Iterable[str]:
        # Re-read from Delta so concurrent writers stay consistent.
        ref = self._table_ref()
        rows = self._spark.sql(
            f"SELECT version FROM {ref} WHERE data_product_id = '{data_product_id}'"
        ).collect()
        return [row[0] for row in rows]

    def latest(self, data_product_id: str) -> Optional[OpenDataProductStandard]:  # noqa: D401
        ref = self._table_ref()
        rows = self._spark.sql(
            f"""
            SELECT json FROM {ref}
            WHERE data_product_id = '{data_product_id}'
            ORDER BY
              CAST(split(version, '\\.')[0] AS INT),
              CAST(split(version, '\\.')[1] AS INT),
              CAST(split(version, '\\.')[2] AS INT)
            DESC LIMIT 1
            """
        ).head(1)
        if not rows:
            return None
        return to_model(self._safe_json(rows[0][0]))

    def get(self, data_product_id: str, version: str) -> OpenDataProductStandard:  # noqa: D401
        ref = self._table_ref()
        rows = self._spark.sql(
            f"SELECT json FROM {ref} WHERE data_product_id = '{data_product_id}' AND version = '{version}'"
        ).head(1)
        if not rows:
            raise FileNotFoundError(f"data product {data_product_id}:{version} not found")
        return to_model(self._safe_json(rows[0][0]))


__all__ = ["DeltaDataProductServiceBackend"]
