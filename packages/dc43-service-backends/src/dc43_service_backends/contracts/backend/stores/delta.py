"""Delta-table-backed contract store."""

from __future__ import annotations

import logging
from typing import List, Optional

try:  # pragma: no cover - optional dependency
    from pyspark.sql import SparkSession
except Exception:  # pragma: no cover
    SparkSession = object  # type: ignore

from ._sql_common import prepare_contract_row
from .interface import ContractStore
from dc43_service_backends.core.odcs import to_model
from dc43_service_backends.core.versioning import version_key
from open_data_contract_standard.model import OpenDataContractStandard  # type: ignore


logger = logging.getLogger(__name__)


class DeltaContractStore(ContractStore):
    """Store contracts inside a Delta table with simple schema."""

    def __init__(
        self,
        spark: SparkSession,
        table: Optional[str] = None,
        path: Optional[str] = None,
        *,
        log_sql: bool = False,
    ):
        """Create the store backed by a UC table or a Delta path."""
        if not (table or path):
            raise ValueError("Provide either a Unity Catalog table name or a Delta path")
        self.spark = spark
        self.table = table
        self.path = path
        self._log_sql = log_sql
        self._ensure_table()

    def _table_ref(self) -> str:
        return self.table if self.table else f"delta.`{self.path}`"

    def _execute_sql(self, statement: str):  # pragma: no cover - small helper
        if self._log_sql:
            logger.info("Spark SQL (contracts): %s", statement.strip())
        return self.spark.sql(statement)

    def _ensure_table(self) -> None:
        ref = self._table_ref()
        if self.table:
            self._execute_sql(
                f"""
                CREATE TABLE IF NOT EXISTS {ref} (
                    contract_id STRING,
                    version STRING,
                    name STRING,
                    description STRING,
                    json STRING,
                    fingerprint STRING,
                    created_at TIMESTAMP
                ) USING DELTA
                PARTITIONED BY (contract_id)
                TBLPROPERTIES (delta.autoOptimize.autoCompact = true)
                """
            )
        else:
            # Path-backed table (Delta path)
            self._execute_sql(
                f"""
                CREATE TABLE IF NOT EXISTS {ref} (
                    contract_id STRING,
                    version STRING,
                    name STRING,
                    description STRING,
                    json STRING,
                    fingerprint STRING,
                    created_at TIMESTAMP
                ) USING DELTA
                LOCATION '{self.path}'
                PARTITIONED BY (contract_id)
                TBLPROPERTIES (delta.autoOptimize.autoCompact = true)
                """
            )

    def put(self, contract: OpenDataContractStandard) -> None:
        """Upsert an ODCS document model into the Delta table."""
        ref = self._table_ref()
        import json

        cid, ver, payload = prepare_contract_row(contract)
        json_str = payload["json"]
        fp = payload["fingerprint"]
        name_val = payload["name"]
        desc_usage = payload.get("description")
        desc_sql = "NULL" if not desc_usage else "'" + str(desc_usage).replace("'", "''") + "'"
        json_sql = json_str.replace("'", "''")
        self._execute_sql(
            f"""
            MERGE INTO {ref} t
            USING (SELECT
                    '{cid}' as contract_id,
                    '{ver}' as version,
                    '{name_val}' as name,
                    {desc_sql} as description,
                    '{json_sql}' as json,
                    '{fp}' as fingerprint,
                    current_timestamp() as created_at) s
            ON t.contract_id = s.contract_id AND t.version = s.version
            WHEN MATCHED THEN UPDATE SET name = s.name, description = s.description, json = s.json, fingerprint = s.fingerprint
            WHEN NOT MATCHED THEN INSERT *
            """
        )

    def get(self, contract_id: str, version: str) -> OpenDataContractStandard:
        """Fetch and parse the ODCS JSON document for the id/version as model."""
        ref = self._table_ref()
        row = self._execute_sql(
            f"SELECT json FROM {ref} WHERE contract_id = '{contract_id}' AND version = '{version}'"
        ).head(1)
        if not row:
            raise KeyError(f"Contract {contract_id}:{version} not found")
        import json

        return to_model(json.loads(row[0][0]))

    def list_contracts(self) -> List[str]:
        """Return all distinct contract identifiers present in the table."""
        ref = self._table_ref()
        rows = self._execute_sql(
            f"SELECT DISTINCT contract_id FROM {ref}"
        ).collect()
        return [r[0] for r in rows]

    def list_versions(self, contract_id: str) -> List[str]:
        """List available versions recorded in the Delta table."""
        ref = self._table_ref()
        rows = self._execute_sql(
            f"SELECT version FROM {ref} WHERE contract_id = '{contract_id}'"
        ).collect()
        return [r[0] for r in rows]

    def latest(self, contract_id: str) -> OpenDataContractStandard | None:
        """Return the latest ODCS model for the given contract id, if any."""
        ref = self._table_ref()
        rows = self._execute_sql(
            f"SELECT version, json FROM {ref} WHERE contract_id = '{contract_id}'"
        ).collect()
        entries: list[tuple[str, object]] = []
        for row in rows:
            if not row:
                continue
            version = str(row[0]).strip()
            if not version:
                continue
            entries.append((version, row[1]))
        if not entries:
            return None
        latest = max(entries, key=lambda row: version_key(row[0]))
        import json

        return to_model(json.loads(latest[1]))


__all__ = ["DeltaContractStore"]
