"""Delta Lake-backed governance persistence for Spark deployments."""

from __future__ import annotations

import json
import logging
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Mapping, Optional, Sequence, TYPE_CHECKING

from dc43_service_clients.data_quality import ValidationResult, coerce_details

from ._metrics import extract_metrics, normalise_metric_value
from ._table_names import (
    derive_related_table_basename,
    derive_related_table_name,
)
from .interface import GovernanceStore


logger = logging.getLogger(__name__)

if TYPE_CHECKING:  # pragma: no cover - import only for static typing
    from pyspark.sql import DataFrame, SparkSession
    from pyspark.sql.functions import Column

try:  # pragma: no cover - optional dependency guard
    from pyspark.sql import DataFrame, SparkSession
    from pyspark.sql.functions import col
    from pyspark.sql.types import (
        BooleanType,
        DoubleType,
        StringType,
        StructField,
        StructType,
    )
    from pyspark.sql.utils import AnalysisException
except ModuleNotFoundError as exc:  # pragma: no cover - allow import without pyspark
    raise ModuleNotFoundError(
        "pyspark is required for Delta governance storage. Install the optional "
        "dependencies with 'pip install dc43-service-backends[spark]'.",
    ) from exc

def _ensure_pyspark_is_remote_shim() -> None:
    """Expose ``is_remote`` for stubbed or legacy pyspark installs."""

    utils_module = sys.modules.get("pyspark.sql.utils")
    if utils_module is None or hasattr(utils_module, "is_remote"):
        return

    def _is_remote(*_: object, **__: object) -> bool:  # pragma: no cover - stub only
        return False

    setattr(utils_module, "is_remote", _is_remote)




_ensure_pyspark_is_remote_shim()


class DeltaGovernanceStore(GovernanceStore):
    """Persist governance artefacts to Delta tables."""

    def __init__(
        self,
        spark: SparkSession,
        *,
        base_path: str | Path | None = None,
        status_table: str | None = None,
        activity_table: str | None = None,
        link_table: str | None = None,
        metrics_table: str | None = None,
        bootstrap_tables: bool = True,
        log_sql: bool = False,
    ) -> None:
        if not base_path and not (status_table and activity_table and link_table):
            raise ValueError(
                "DeltaGovernanceStore requires either a base_path or explicit table names",
            )
        self._spark = spark
        self._base_path = Path(base_path).expanduser() if base_path else None
        self._status_table = status_table
        self._activity_table = activity_table
        self._link_table = link_table
        self._metrics_table = metrics_table
        self._log_sql = log_sql
        if not self._metrics_table and self._base_path is None:
            reference_table = status_table or activity_table or link_table
            if reference_table:
                self._metrics_table = self._derive_related_table_name(
                    reference_table, "metrics"
                )

        if bootstrap_tables:
            self.bootstrap()

    _derive_related_table_name = staticmethod(derive_related_table_name)
    _derive_related_table_basename = staticmethod(derive_related_table_basename)

    _STATUS_SCHEMA = StructType(
        [
            StructField("dataset_id", StringType(), False),
            StructField("dataset_version", StringType(), True),
            StructField("contract_id", StringType(), False),
            StructField("contract_version", StringType(), True),
            StructField("recorded_at", StringType(), False),
            StructField("deleted", BooleanType(), False),
            StructField("payload", StringType(), False),
        ]
    )
    _METRIC_SCHEMA = StructType(
        [
            StructField("dataset_id", StringType(), False),
            StructField("dataset_version", StringType(), True),
            StructField("contract_id", StringType(), False),
            StructField("contract_version", StringType(), True),
            StructField("status_recorded_at", StringType(), False),
            StructField("metric_key", StringType(), False),
            StructField("metric_value", StringType(), True),
            StructField("metric_numeric_value", DoubleType(), True),
        ]
    )
    _LINK_SCHEMA = StructType(
        [
            StructField("dataset_id", StringType(), False),
            StructField("dataset_version", StringType(), True),
            StructField("contract_id", StringType(), False),
            StructField("contract_version", StringType(), True),
            StructField("linked_at", StringType(), False),
        ]
    )
    _ACTIVITY_SCHEMA = StructType(
        [
            StructField("dataset_id", StringType(), False),
            StructField("dataset_version", StringType(), True),
            StructField("contract_id", StringType(), False),
            StructField("contract_version", StringType(), True),
            StructField("recorded_at", StringType(), False),
            StructField("payload", StringType(), False),
            StructField("lineage_event", StringType(), True),
        ]
    )

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------
    def _now(self) -> str:
        return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")

    def _table_path(self, name: str) -> str:
        assert self._base_path is not None
        path = self._base_path / name
        path.mkdir(parents=True, exist_ok=True)
        return str(path)

    @staticmethod
    def _escape_identifier(value: str) -> str:
        return value.replace("`", "``")

    @staticmethod
    def _escape_literal(value: str) -> str:
        return value.replace("'", "''")

    @staticmethod
    def _column_condition(column: str, value: Optional[str]) -> str:
        if value is None:
            return f"{column} IS NULL"
        escaped = DeltaGovernanceStore._escape_literal(str(value))
        return f"{column} = '{escaped}'"

    @staticmethod
    def _sql_target_identifier(table: str | None, folder: str | None) -> str | None:
        if table:
            return table
        if folder:
            return f"delta.`{DeltaGovernanceStore._escape_identifier(str(folder))}`"
        return None

    def _execute_sql(self, statement: str):  # pragma: no cover - logging shim
        if self._log_sql:
            logger.info("Spark SQL (governance): %s", statement.strip())
        return self._spark.sql(statement)

    def _purge_status_entries(
        self,
        *,
        dataset_id: str,
        dataset_version: str,
        table: str | None,
        folder: str | None,
    ) -> None:
        target = self._sql_target_identifier(table, folder)
        if not target:
            return
        condition = " AND ".join(
            (
                self._column_condition("dataset_id", dataset_id),
                self._column_condition("dataset_version", dataset_version),
            )
        )
        statement = f"DELETE FROM {target} WHERE {condition}"
        try:
            self._execute_sql(statement)
        except Exception:  # pragma: no cover - delta deletes may fail on legacy runtimes
            logger.exception(
                "Failed to purge existing status rows for %s@%s",
                dataset_id,
                dataset_version,
            )

    def _delta_folder_exists(self, folder: str | None) -> bool:
        if not folder:
            return False

        if hasattr(self._spark, "_jvm") and hasattr(self._spark, "_jsc"):
            try:
                jvm = self._spark._jvm
                jpath = jvm.org.apache.hadoop.fs.Path(str(folder))
                fs = jpath.getFileSystem(self._spark._jsc.hadoopConfiguration())
                delta_log_path = jvm.org.apache.hadoop.fs.Path(jpath, "_delta_log")
                return fs.exists(delta_log_path)
            except Exception:  # pragma: no cover - fall back to local checks
                pass

        path = Path(folder)
        return (path / "_delta_log").exists()

    def _table_exists(self, table: str | None) -> bool:
        if not table:
            return False

        if hasattr(self._spark, "sql"):
            parts = table.split(".")
            if len(parts) == 3:
                catalog, schema, name = parts

                def _escape(value: str) -> str:
                    return value.replace("'", "''")

                try:
                    query = (
                        "SELECT 1 FROM system.information_schema.tables "
                        f"WHERE table_catalog = '{_escape(catalog)}' "
                        f"AND table_schema = '{_escape(schema)}' "
                        f"AND table_name = '{_escape(name)}' "
                        "LIMIT 1"
                    )
                    rows = self._execute_sql(query).collect()
                    if rows:
                        return True
                except Exception:  # pragma: no cover - fall back to catalog lookup
                    pass

        return bool(self._spark.catalog.tableExists(table))


    def _ensure_delta_target(
        self,
        *,
        table: str | None,
        folder: str | None,
        schema: StructType,
    ) -> None:
        table_exists = self._table_exists(table)
        if table_exists:
            return
        folder_exists = self._delta_folder_exists(folder)
        if not table and folder_exists:
            return

        df = self._spark.createDataFrame([], schema)
        writer = (
            df.write.format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "true")
        )
        if table:
            writer.saveAsTable(table)
        elif folder:
            Path(folder).mkdir(parents=True, exist_ok=True)
            writer.option("path", folder).save()
        else:  # pragma: no cover - defensive guard
            raise RuntimeError("DeltaGovernanceStore bootstrap target unspecified")

    def bootstrap(self) -> None:
        """Ensure Delta tables or folders exist for governance artefacts."""

        targets = (
            ("status", self._status_table, self._STATUS_SCHEMA),
            ("links", self._link_table, self._LINK_SCHEMA),
            ("activity", self._activity_table, self._ACTIVITY_SCHEMA),
            ("metrics", self._metrics_table, self._METRIC_SCHEMA),
        )
        for name, table, schema in targets:
            folder = self._table_path(name) if self._base_path else None
            if not table and not folder:
                continue
            self._ensure_delta_target(table=table, folder=folder, schema=schema)

    def _write(self, df: DataFrame, *, table: str | None, folder: str | None) -> None:
        writer = df.write.format("delta").mode("append")
        if table:
            writer.saveAsTable(table)
        elif folder:
            writer.option("path", folder).save()
        else:
            raise RuntimeError("DeltaGovernanceStore writer requires a table or folder")

    def _read(self, *, table: str | None, folder: str | None) -> DataFrame | None:
        try:
            if table:
                return self._spark.table(table)
            if folder:
                if not self._delta_folder_exists(folder):
                    return None
                return self._spark.read.format("delta").load(folder)
        except AnalysisException:
            return None
        return None

    # ------------------------------------------------------------------
    # Status persistence
    # ------------------------------------------------------------------
    def save_status(
        self,
        *,
        contract_id: str,
        contract_version: str,
        dataset_id: str,
        dataset_version: str,
        status: ValidationResult | None,
    ) -> None:
        recorded_at = self._now()
        folder = self._table_path("status") if self._base_path else None
        self._purge_status_entries(
            dataset_id=dataset_id,
            dataset_version=dataset_version,
            table=self._status_table,
            folder=folder,
        )
        details_payload = status.details if status else {}
        payload = {
            "dataset_id": dataset_id,
            "dataset_version": dataset_version,
            "contract_id": contract_id,
            "contract_version": contract_version,
            "recorded_at": recorded_at,
            "deleted": status is None,
            "payload": json.dumps(
                {
                    "status": status.status if status else "unknown",
                    "reason": status.reason if status else None,
                    "details": details_payload,
                    "contract_id": contract_id,
                    "contract_version": contract_version,
                    "dataset_id": dataset_id,
                    "dataset_version": dataset_version,
                    "deleted": status is None,
                }
            ),
        }
        df = self._spark.createDataFrame([payload], schema=self._STATUS_SCHEMA)
        self._write(df, table=self._status_table, folder=folder)

        if status is None:
            return

        metrics_map = extract_metrics(status)
        metrics_records = []
        for key, value in metrics_map.items():
            value_payload, numeric_value = normalise_metric_value(value)
            metrics_records.append(
                {
                    "dataset_id": dataset_id,
                    "dataset_version": dataset_version,
                    "contract_id": contract_id,
                    "contract_version": contract_version,
                    "status_recorded_at": recorded_at,
                    "metric_key": str(key),
                    "metric_value": value_payload,
                    "metric_numeric_value": numeric_value,
                }
            )

        if not metrics_records:
            return

        metrics_folder = self._table_path("metrics") if self._base_path else None
        if not self._metrics_table and not metrics_folder:
            return

        metrics_df = self._spark.createDataFrame(metrics_records, self._METRIC_SCHEMA)
        self._write(metrics_df, table=self._metrics_table, folder=metrics_folder)

    def load_status(
        self,
        *,
        contract_id: str,
        contract_version: str,
        dataset_id: str,
        dataset_version: str,
    ) -> ValidationResult | None:
        folder = self._table_path("status") if self._base_path else None
        df = self._read(table=self._status_table, folder=folder)
        if df is None:
            return None
        rows = (
            df.filter((col("dataset_id") == dataset_id) & (col("dataset_version") == dataset_version))
            .orderBy(col("recorded_at").desc())
            .limit(1)
            .collect()
        )
        if not rows:
            return None
        row = rows[0]
        record = json.loads(row.payload)
        if record.get("deleted"):
            return None
        linked = record.get("contract_id"), record.get("contract_version")
        if linked != (contract_id, contract_version):
            reason = (
                f"dataset linked to contract {linked[0]}:{linked[1]}"
                if all(linked)
                else "dataset linked to a different contract"
            )
            return ValidationResult(status="block", reason=reason, details=record)
        return ValidationResult(
            status=str(record.get("status", "unknown")),
            reason=str(record.get("reason")) if record.get("reason") else None,
            details=coerce_details(record.get("details")),
        )

    def load_status_matrix_entries(
        self,
        *,
        dataset_id: str,
        dataset_versions: Sequence[str] | None = None,
        contract_ids: Sequence[str] | None = None,
    ) -> Sequence[Mapping[str, object]]:
        folder = self._table_path("status") if self._base_path else None
        df = self._read(table=self._status_table, folder=folder)
        if df is None:
            return ()
        condition = col("dataset_id") == dataset_id
        versions = [str(value) for value in (dataset_versions or []) if str(value)]
        if versions:
            condition = condition & col("dataset_version").isin(versions)
        contracts = [str(value) for value in (contract_ids or []) if str(value)]
        if contracts:
            condition = condition & col("contract_id").isin(contracts)
        rows = df.filter(condition).collect()
        entries: list[Mapping[str, object]] = []
        for row in rows:
            if getattr(row, "deleted", False):
                continue
            record = json.loads(row.payload)
            status = ValidationResult(
                status=str(record.get("status", "unknown")),
                reason=str(record.get("reason")) if record.get("reason") else None,
                details=coerce_details(record.get("details")),
            )
            entries.append(
                {
                    "dataset_id": row.dataset_id,
                    "dataset_version": row.dataset_version,
                    "contract_id": row.contract_id,
                    "contract_version": row.contract_version,
                    "status": status,
                }
            )
        return tuple(entries)

    # ------------------------------------------------------------------
    # Dataset links
    # ------------------------------------------------------------------
    def link_dataset_contract(
        self,
        *,
        dataset_id: str,
        dataset_version: str,
        contract_id: str,
        contract_version: str,
    ) -> None:
        payload = {
            "dataset_id": dataset_id,
            "dataset_version": dataset_version,
            "contract_id": contract_id,
            "contract_version": contract_version,
            "linked_at": self._now(),
        }
        df = self._spark.createDataFrame([payload], schema=self._LINK_SCHEMA)
        folder = self._table_path("links") if self._base_path else None
        self._write(df, table=self._link_table, folder=folder)

    def get_linked_contract_version(
        self,
        *,
        dataset_id: str,
        dataset_version: Optional[str] = None,
    ) -> str | None:
        folder = self._table_path("links") if self._base_path else None
        df = self._read(table=self._link_table, folder=folder)
        if df is None:
            return None
        condition = col("dataset_id") == dataset_id
        if dataset_version is not None:
            condition = condition & (col("dataset_version") == dataset_version)
        rows = df.filter(condition).orderBy(col("linked_at").desc()).limit(1).collect()
        if not rows:
            return None
        row = rows[0]
        cid = row.contract_id
        cver = row.contract_version
        if cid and cver:
            return f"{cid}:{cver}"
        return None

    # ------------------------------------------------------------------
    # Pipeline activity
    # ------------------------------------------------------------------
    def record_pipeline_event(
        self,
        *,
        contract_id: str,
        contract_version: str,
        dataset_id: str,
        dataset_version: str,
        event: Mapping[str, object],
        lineage_event: Mapping[str, object] | None = None,
    ) -> None:
        payload = {
            "dataset_id": dataset_id,
            "dataset_version": dataset_version,
            "contract_id": contract_id,
            "contract_version": contract_version,
            "recorded_at": self._now(),
            "payload": json.dumps(dict(event)),
            "lineage_event": json.dumps(dict(lineage_event)) if lineage_event is not None else None,
        }
        df = self._spark.createDataFrame([payload], schema=self._ACTIVITY_SCHEMA)
        folder = self._table_path("activity") if self._base_path else None
        self._write(df, table=self._activity_table, folder=folder)

    def list_datasets(self) -> Sequence[str]:
        _ensure_pyspark_is_remote_shim()
        folder = self._table_path("activity") if self._base_path else None
        df = self._read(table=self._activity_table, folder=folder)
        if df is None:
            return []
        rows = df.select("dataset_id").distinct().orderBy(col("dataset_id")).collect()
        return [row.dataset_id for row in rows if getattr(row, "dataset_id", None)]

    def load_pipeline_activity(
        self,
        *,
        dataset_id: str,
        dataset_version: Optional[str] = None,
    ) -> Sequence[Mapping[str, object]]:
        _ensure_pyspark_is_remote_shim()
        folder = self._table_path("activity") if self._base_path else None
        df = self._read(table=self._activity_table, folder=folder)
        if df is None:
            return []
        condition = col("dataset_id") == dataset_id
        if dataset_version is not None:
            condition = condition & (col("dataset_version") == dataset_version)
        rows = df.filter(condition).orderBy(col("recorded_at")).collect()
        aggregated: dict[str, dict[str, object]] = {}
        for row in rows:
            record = aggregated.setdefault(
                row.dataset_version,
                {
                    "dataset_id": row.dataset_id,
                    "dataset_version": row.dataset_version,
                    "contract_id": row.contract_id,
                    "contract_version": row.contract_version,
                    "events": [],
                },
            )
            try:
                event_payload = json.loads(row.payload)
            except json.JSONDecodeError:
                event_payload = {}
            if isinstance(event_payload, dict):
                record.setdefault("events", []).append(event_payload)
            lineage_raw = getattr(row, "lineage_event", None)
            if lineage_raw:
                try:
                    lineage_payload = json.loads(lineage_raw)
                except json.JSONDecodeError:
                    lineage_payload = None
                if isinstance(lineage_payload, dict):
                    record["lineage_event"] = lineage_payload
        ordered = sorted(aggregated.values(), key=lambda item: str(item.get("dataset_version", "")))
        if dataset_version is not None:
            return [ordered[0]] if ordered else []
        return ordered

    # ------------------------------------------------------------------
    # Metrics retrieval
    # ------------------------------------------------------------------
    def load_metrics(
        self,
        *,
        dataset_id: str,
        dataset_version: Optional[str] = None,
        contract_id: Optional[str] = None,
        contract_version: Optional[str] = None,
    ) -> Sequence[Mapping[str, object]]:
        _ensure_pyspark_is_remote_shim()
        metrics_folder = self._table_path("metrics") if self._base_path else None
        df = self._read(table=self._metrics_table, folder=metrics_folder)
        if df is None:
            return []

        condition = col("dataset_id") == dataset_id
        if dataset_version is not None:
            condition = condition & (col("dataset_version") == dataset_version)
        if contract_id is not None:
            condition = condition & (col("contract_id") == contract_id)
        if contract_version is not None:
            condition = condition & (col("contract_version") == contract_version)

        rows = (
            df.filter(condition)
            .orderBy(col("status_recorded_at"), col("metric_key"))
            .collect()
        )
        entries: list[Mapping[str, object]] = []
        for row in rows:
            payload = row.asDict(recursive=False)
            numeric_value = payload.get("metric_numeric_value")
            if numeric_value is not None:
                try:
                    payload["metric_numeric_value"] = float(numeric_value)
                except (TypeError, ValueError):  # pragma: no cover - defensive
                    payload["metric_numeric_value"] = None
            entries.append(payload)
        return entries


__all__ = ["DeltaGovernanceStore"]
