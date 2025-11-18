"""SQL-backed governance persistence using SQLAlchemy."""

from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from typing import Mapping, Optional, Sequence

from sqlalchemy import (
    Column,
    Float,
    MetaData,
    String,
    Table,
    Text,
    inspect,
    select,
    text,
)
from sqlalchemy.engine import Engine
from sqlalchemy.exc import SQLAlchemyError

from dc43_service_clients.data_quality import ValidationResult, coerce_details

from ._metrics import extract_metrics, normalise_metric_value
from ._table_names import derive_related_table_name
from .interface import GovernanceStore


logger = logging.getLogger(__name__)


class SQLGovernanceStore(GovernanceStore):
    """Persist governance artefacts to relational databases."""

    def __init__(
        self,
        engine: Engine,
        *,
        schema: str | None = None,
        status_table: str = "dq_status",
        activity_table: str = "dq_activity",
        link_table: str = "dq_dataset_contract_links",
        metrics_table: str | None = None,
    ) -> None:
        self._engine = engine
        metadata = MetaData(schema=schema)
        resolved_metrics_table = metrics_table
        if not resolved_metrics_table and status_table:
            resolved_metrics_table = derive_related_table_name(status_table, "metrics")
        metrics_table_name = resolved_metrics_table or "dq_metrics"
        self._status = Table(
            status_table,
            metadata,
            Column("dataset_id", String, primary_key=True),
            Column("dataset_version", String, primary_key=True),
            Column("contract_id", String, nullable=False),
            Column("contract_version", String, nullable=False),
            Column("payload", Text, nullable=False),
            Column("recorded_at", String, nullable=False),
        )
        self._activity = Table(
            activity_table,
            metadata,
            Column("dataset_id", String, primary_key=True),
            Column("dataset_version", String, primary_key=True),
            Column("payload", Text, nullable=False),
            Column("updated_at", String, nullable=False),
        )
        self._links = Table(
            link_table,
            metadata,
            Column("dataset_id", String, primary_key=True),
            Column("dataset_version", String, primary_key=True),
            Column("contract_id", String, nullable=False),
            Column("contract_version", String, nullable=False),
            Column("linked_at", String, nullable=False),
        )
        self._metrics = Table(
            metrics_table_name,
            metadata,
            Column("dataset_id", String, nullable=False),
            Column("dataset_version", String, nullable=True),
            Column("contract_id", String, nullable=True),
            Column("contract_version", String, nullable=True),
            Column("status_recorded_at", String, nullable=False),
            Column("metric_key", String, nullable=False),
            Column("metric_value", Text, nullable=True),
            Column("metric_numeric_value", Float, nullable=True),
        )
        metadata.create_all(engine)
        inspector = inspect(engine)
        try:
            activity_columns = {
                str(column["name"])
                for column in inspector.get_columns(activity_table, schema=schema)
            }
        except SQLAlchemyError:
            activity_columns = {column.name for column in self._activity.columns}
        self._activity_has_updated_at = "updated_at" in activity_columns

        try:
            link_columns = {
                str(column["name"])
                for column in inspector.get_columns(link_table, schema=schema)
            }
        except SQLAlchemyError:
            link_columns = {column.name for column in self._links.columns}
        self._links_has_linked_at = "linked_at" in link_columns

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------
    def _now(self) -> str:
        return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")

    def _load_payload(
        self,
        table: Table,
        *,
        dataset_id: str,
        dataset_version: str,
        sort_column: Column | None = None,
    ) -> dict[str, object] | None:
        base_stmt = (
            select(table.c.payload)
            .where(table.c.dataset_id == dataset_id)
            .where(table.c.dataset_version == dataset_version)
            .limit(1)
        )
        stmt = base_stmt
        if sort_column is not None:
            stmt = base_stmt.order_by(sort_column.desc())
        with self._engine.begin() as conn:
            try:
                result = conn.execute(stmt).scalars().first()
            except SQLAlchemyError:
                if sort_column is None:
                    raise
                logger.exception(
                    "Failed to order pipeline activity for %s@%s by %s; retrying without sort",
                    dataset_id,
                    dataset_version,
                    getattr(sort_column, "key", sort_column),
                )
                result = conn.execute(base_stmt).scalars().first()
        if not result:
            return None
        try:
            data = json.loads(result)
        except json.JSONDecodeError:
            return None
        if isinstance(data, dict):
            return data
        return None

    def _write_payload(
        self,
        table: Table,
        *,
        dataset_id: str,
        dataset_version: str,
        payload: Mapping[str, object],
        extra: Mapping[str, object] | None = None,
    ) -> None:
        record = dict(payload)
        if extra:
            record.update(extra)
        serialized = json.dumps(record)
        filtered_extra = {
            key: value for key, value in (extra or {}).items() if key in table.c
        }
        base_values = {
            "dataset_id": dataset_id,
            "dataset_version": dataset_version,
            "payload": serialized,
        }
        with self._engine.begin() as conn:
            conn.execute(
                table.delete()
                .where(table.c.dataset_id == dataset_id)
                .where(table.c.dataset_version == dataset_version)
            )
            try:
                conn.execute(table.insert().values(**base_values, **filtered_extra))
            except SQLAlchemyError:
                if filtered_extra:
                    logger.exception(
                        "Falling back to storing %s@%s without auxiliary columns",
                        dataset_id,
                        dataset_version,
                    )
                    fallback_columns = [
                        key
                        for key in ("dataset_id", "dataset_version", "payload")
                        if key in base_values
                    ]
                    if not fallback_columns:
                        raise
                    statement = text(
                        "INSERT INTO "
                        f"{table.fullname} ({', '.join(fallback_columns)}) "
                        f"VALUES ({', '.join(f':{name}' for name in fallback_columns)})"
                    )
                    conn.execute(
                        statement,
                        {key: base_values[key] for key in fallback_columns},
                    )
                else:
                    raise

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
        if status is None:
            with self._engine.begin() as conn:
                conn.execute(
                    self._status.delete()
                    .where(self._status.c.dataset_id == dataset_id)
                    .where(self._status.c.dataset_version == dataset_version)
                )
            return

        recorded_at = self._now()
        details_payload = status.details
        payload = {
            "contract_id": contract_id,
            "contract_version": contract_version,
            "dataset_id": dataset_id,
            "dataset_version": dataset_version,
            "status": status.status,
            "reason": status.reason,
            "details": details_payload,
        }
        self._write_payload(
            self._status,
            dataset_id=dataset_id,
            dataset_version=dataset_version,
            payload=payload,
            extra={
                "contract_id": contract_id,
                "contract_version": contract_version,
                "recorded_at": recorded_at,
            },
        )

        metrics_map = extract_metrics(status)
        metrics_entries = []
        for key, value in metrics_map.items():
            serialised_value, numeric_value = normalise_metric_value(value)
            metrics_entries.append(
                {
                    "dataset_id": dataset_id,
                    "dataset_version": dataset_version,
                    "contract_id": contract_id,
                    "contract_version": contract_version,
                    "status_recorded_at": recorded_at,
                    "metric_key": str(key),
                    "metric_value": serialised_value,
                    "metric_numeric_value": numeric_value,
                }
            )
        if metrics_entries:
            with self._engine.begin() as conn:
                conn.execute(self._metrics.insert(), metrics_entries)

    def load_status(
        self,
        *,
        contract_id: str,
        contract_version: str,
        dataset_id: str,
        dataset_version: str,
    ) -> ValidationResult | None:
        payload = self._load_payload(
            self._status,
            dataset_id=dataset_id,
            dataset_version=dataset_version,
            sort_column=self._status.c.recorded_at,
        )
        if not payload:
            return None
        linked = payload.get("contract_id"), payload.get("contract_version")
        if linked != (contract_id, contract_version):
            reason = (
                f"dataset linked to contract {linked[0]}:{linked[1]}"
                if all(linked)
                else "dataset linked to a different contract"
            )
            return ValidationResult(status="block", reason=reason, details=payload)
        return ValidationResult(
            status=str(payload.get("status", "unknown")),
            reason=str(payload.get("reason")) if payload.get("reason") else None,
            details=coerce_details(payload.get("details")),
        )

    def load_status_matrix_entries(
        self,
        *,
        dataset_id: str,
        dataset_versions: Sequence[str] | None = None,
        contract_ids: Sequence[str] | None = None,
    ) -> Sequence[Mapping[str, object]]:
        versions = [str(value) for value in (dataset_versions or []) if str(value)]
        contracts = [str(value) for value in (contract_ids or []) if str(value)]
        statement = select(
            self._status.c.dataset_id,
            self._status.c.dataset_version,
            self._status.c.contract_id,
            self._status.c.contract_version,
            self._status.c.payload,
        ).where(self._status.c.dataset_id == dataset_id)
        if versions:
            statement = statement.where(self._status.c.dataset_version.in_(versions))
        if contracts:
            statement = statement.where(self._status.c.contract_id.in_(contracts))

        entries: list[Mapping[str, object]] = []
        with self._engine.connect() as conn:
            rows = conn.execute(statement).fetchall()
        for row in rows:
            raw_payload = row.payload or "{}"
            try:
                payload = json.loads(raw_payload)
            except (TypeError, ValueError):
                payload = {}
            status = ValidationResult(
                status=str(payload.get("status", "unknown")),
                reason=str(payload.get("reason")) if payload.get("reason") else None,
                details=coerce_details(payload.get("details")),
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
        linked_at = self._now()
        with self._engine.begin() as conn:
            conn.execute(
                self._links.delete()
                .where(self._links.c.dataset_id == dataset_id)
                .where(self._links.c.dataset_version == dataset_version)
            )
            conn.execute(
                self._links.insert().values(
                    dataset_id=dataset_id,
                    dataset_version=dataset_version,
                    contract_id=contract_id,
                    contract_version=contract_version,
                    linked_at=linked_at,
                )
            )

    def get_linked_contract_version(
        self,
        *,
        dataset_id: str,
        dataset_version: Optional[str] = None,
    ) -> str | None:
        if dataset_version is not None:
            stmt = (
                select(
                    self._links.c.contract_id,
                    self._links.c.contract_version,
                )
                .where(self._links.c.dataset_id == dataset_id)
                .where(self._links.c.dataset_version == dataset_version)
            )
            if self._links_has_linked_at:
                stmt = stmt.order_by(self._links.c.linked_at.desc())
            stmt = stmt.limit(1)
            with self._engine.begin() as conn:
                row = conn.execute(stmt).first()
            if row and row.contract_id and row.contract_version:
                return f"{row.contract_id}:{row.contract_version}"
            return None

        stmt = select(self._links.c.contract_id, self._links.c.contract_version).where(
            self._links.c.dataset_id == dataset_id
        )
        with self._engine.begin() as conn:
            row = conn.execute(stmt).first()
        if row and row.contract_id and row.contract_version:
            return f"{row.contract_id}:{row.contract_version}"
        return None

    # ------------------------------------------------------------------
    # Metrics
    # ------------------------------------------------------------------
    def load_metrics(
        self,
        *,
        dataset_id: str,
        dataset_version: Optional[str] = None,
        contract_id: Optional[str] = None,
        contract_version: Optional[str] = None,
    ) -> Sequence[Mapping[str, object]]:
        stmt = select(
            self._metrics.c.dataset_id,
            self._metrics.c.dataset_version,
            self._metrics.c.contract_id,
            self._metrics.c.contract_version,
            self._metrics.c.status_recorded_at,
            self._metrics.c.metric_key,
            self._metrics.c.metric_value,
            self._metrics.c.metric_numeric_value,
        ).where(self._metrics.c.dataset_id == dataset_id)
        if dataset_version is not None:
            stmt = stmt.where(self._metrics.c.dataset_version == dataset_version)
        if contract_id is not None:
            stmt = stmt.where(self._metrics.c.contract_id == contract_id)
        if contract_version is not None:
            stmt = stmt.where(self._metrics.c.contract_version == contract_version)
        stmt = stmt.order_by(
            self._metrics.c.status_recorded_at,
            self._metrics.c.metric_key,
        )

        records: list[Mapping[str, object]] = []
        with self._engine.begin() as conn:
            for row in conn.execute(stmt).all():
                payload = dict(row._mapping)
                numeric = payload.get("metric_numeric_value")
                if numeric is not None:
                    try:
                        payload["metric_numeric_value"] = float(numeric)
                    except (TypeError, ValueError):  # pragma: no cover - defensive
                        payload["metric_numeric_value"] = None
                records.append(payload)
        return records

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
        sort_column = self._activity.c.updated_at if self._activity_has_updated_at else None
        record = self._load_payload(
            self._activity,
            dataset_id=dataset_id,
            dataset_version=dataset_version,
            sort_column=sort_column,
        )
        if not isinstance(record, dict):
            record = {
                "dataset_id": dataset_id,
                "dataset_version": dataset_version,
                "contract_id": contract_id,
                "contract_version": contract_version,
                "events": [],
            }
        events = list(record.get("events") or [])
        events.append(dict(event))
        record["events"] = events
        if lineage_event is not None:
            record["lineage_event"] = dict(lineage_event)
        record["contract_id"] = contract_id
        record["contract_version"] = contract_version
        extra: Mapping[str, object] | None = None
        if self._activity_has_updated_at:
            extra = {"updated_at": self._now()}
        self._write_payload(
            self._activity,
            dataset_id=dataset_id,
            dataset_version=dataset_version,
            payload=record,
            extra=extra,
        )

    def list_datasets(self) -> Sequence[str]:
        stmt = select(self._activity.c.dataset_id).distinct().order_by(
            self._activity.c.dataset_id
        )
        datasets: list[str] = []
        with self._engine.begin() as conn:
            for (dataset_id,) in conn.execute(stmt).all():
                if isinstance(dataset_id, str):
                    datasets.append(dataset_id)
        return datasets

    def load_pipeline_activity(
        self,
        *,
        dataset_id: str,
        dataset_version: Optional[str] = None,
    ) -> Sequence[Mapping[str, object]]:
        if dataset_version is not None:
            sort_column = self._activity.c.updated_at if self._activity_has_updated_at else None
            record = self._load_payload(
                self._activity,
                dataset_id=dataset_id,
                dataset_version=dataset_version,
                sort_column=sort_column,
            )
            if record:
                record.setdefault("dataset_id", dataset_id)
                record.setdefault("dataset_version", dataset_version)
                return [record]
            return []

        stmt = select(self._activity.c.dataset_version, self._activity.c.payload).where(
            self._activity.c.dataset_id == dataset_id
        )
        entries: list[Mapping[str, object]] = []
        with self._engine.begin() as conn:
            for row in conn.execute(stmt).all():
                payload = row.payload
                try:
                    record = json.loads(payload)
                except json.JSONDecodeError:
                    continue
                if isinstance(record, dict):
                    record.setdefault("dataset_id", dataset_id)
                    version = getattr(row, "dataset_version", None)
                    if isinstance(version, str) and version:
                        record.setdefault("dataset_version", version)
                    entries.append(record)
        entries.sort(
            key=lambda item: str(
                (item.get("events") or [{}])[-1].get("recorded_at", "")
                if isinstance(item.get("events"), list) and item["events"]
                else ""
            )
        )
        return entries


__all__ = ["SQLGovernanceStore"]
