"""Delta Lake-backed governance persistence for Spark deployments."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Mapping, Optional, Sequence

from dc43_service_clients.data_quality import ValidationResult, coerce_details

from .interface import GovernanceStore

try:  # pragma: no cover - optional dependency guard
    from pyspark.sql import DataFrame, SparkSession
    from pyspark.sql.functions import col
    from pyspark.sql.utils import AnalysisException
except ModuleNotFoundError as exc:  # pragma: no cover - surfaced via builder guards
    raise ModuleNotFoundError(
        "pyspark is required to use the DeltaGovernanceStore."
    ) from exc


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
                if not Path(folder).exists():
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
        payload = {
            "dataset_id": dataset_id,
            "dataset_version": dataset_version,
            "contract_id": contract_id,
            "contract_version": contract_version,
            "recorded_at": self._now(),
            "deleted": status is None,
            "payload": json.dumps(
                {
                    "status": status.status if status else "unknown",
                    "reason": status.reason if status else None,
                    "details": status.details if status else {},
                    "contract_id": contract_id,
                    "contract_version": contract_version,
                    "dataset_id": dataset_id,
                    "dataset_version": dataset_version,
                    "deleted": status is None,
                }
            ),
        }
        df = self._spark.createDataFrame([payload])
        folder = self._table_path("status") if self._base_path else None
        self._write(df, table=self._status_table, folder=folder)

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
        df = self._spark.createDataFrame([payload])
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
    ) -> None:
        payload = {
            "dataset_id": dataset_id,
            "dataset_version": dataset_version,
            "contract_id": contract_id,
            "contract_version": contract_version,
            "recorded_at": self._now(),
            "payload": json.dumps(dict(event)),
        }
        df = self._spark.createDataFrame([payload])
        folder = self._table_path("activity") if self._base_path else None
        self._write(df, table=self._activity_table, folder=folder)

    def load_pipeline_activity(
        self,
        *,
        dataset_id: str,
        dataset_version: Optional[str] = None,
    ) -> Sequence[Mapping[str, object]]:
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
        ordered = sorted(aggregated.values(), key=lambda item: str(item.get("dataset_version", "")))
        if dataset_version is not None:
            return [ordered[0]] if ordered else []
        return ordered


__all__ = ["DeltaGovernanceStore"]
