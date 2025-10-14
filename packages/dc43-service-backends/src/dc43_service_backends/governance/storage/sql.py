"""SQL-backed governance persistence using SQLAlchemy."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Mapping, Optional, Sequence

from sqlalchemy import Column, MetaData, String, Table, Text, select
from sqlalchemy.engine import Engine

from dc43_service_clients.data_quality import ValidationResult, coerce_details

from .interface import GovernanceStore


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
    ) -> None:
        self._engine = engine
        metadata = MetaData(schema=schema)
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
        metadata.create_all(engine)

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------
    def _now(self) -> str:
        return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")

    def _load_payload(self, table: Table, *, dataset_id: str, dataset_version: str) -> dict[str, object] | None:
        stmt = (
            select(table.c.payload)
            .where(table.c.dataset_id == dataset_id)
            .where(table.c.dataset_version == dataset_version)
        )
        with self._engine.begin() as conn:
            result = conn.execute(stmt).scalar_one_or_none()
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
        with self._engine.begin() as conn:
            conn.execute(
                table.delete()
                .where(table.c.dataset_id == dataset_id)
                .where(table.c.dataset_version == dataset_version)
            )
            conn.execute(
                table.insert().values(
                    dataset_id=dataset_id,
                    dataset_version=dataset_version,
                    payload=serialized,
                    **{key: value for key, value in (extra or {}).items() if key in table.c},
                )
            )

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

        payload = {
            "contract_id": contract_id,
            "contract_version": contract_version,
            "dataset_id": dataset_id,
            "dataset_version": dataset_version,
            "status": status.status,
            "reason": status.reason,
            "details": status.details,
        }
        self._write_payload(
            self._status,
            dataset_id=dataset_id,
            dataset_version=dataset_version,
            payload=payload,
            extra={
                "contract_id": contract_id,
                "contract_version": contract_version,
                "recorded_at": self._now(),
            },
        )

    def load_status(
        self,
        *,
        contract_id: str,
        contract_version: str,
        dataset_id: str,
        dataset_version: str,
    ) -> ValidationResult | None:
        payload = self._load_payload(
            self._status, dataset_id=dataset_id, dataset_version=dataset_version
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
            "contract_id": contract_id,
            "contract_version": contract_version,
            "dataset_version": dataset_version,
            "linked_at": self._now(),
        }
        self._write_payload(
            self._links,
            dataset_id=dataset_id,
            dataset_version=dataset_version,
            payload=payload,
            extra={
                "contract_id": contract_id,
                "contract_version": contract_version,
                "linked_at": self._now(),
            },
        )

    def get_linked_contract_version(
        self,
        *,
        dataset_id: str,
        dataset_version: Optional[str] = None,
    ) -> str | None:
        if dataset_version is not None:
            payload = self._load_payload(
                self._links,
                dataset_id=dataset_id,
                dataset_version=dataset_version,
            )
            if payload:
                cid = payload.get("contract_id")
                cver = payload.get("contract_version")
                if cid and cver:
                    return f"{cid}:{cver}"
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
        record = self._load_payload(
            self._activity, dataset_id=dataset_id, dataset_version=dataset_version
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
        record["contract_id"] = contract_id
        record["contract_version"] = contract_version
        self._write_payload(
            self._activity,
            dataset_id=dataset_id,
            dataset_version=dataset_version,
            payload=record,
            extra={"updated_at": self._now()},
        )

    def load_pipeline_activity(
        self,
        *,
        dataset_id: str,
        dataset_version: Optional[str] = None,
    ) -> Sequence[Mapping[str, object]]:
        if dataset_version is not None:
            record = self._load_payload(
                self._activity,
                dataset_id=dataset_id,
                dataset_version=dataset_version,
            )
            if record:
                return [record]
            return []

        stmt = select(self._activity.c.payload).where(
            self._activity.c.dataset_id == dataset_id
        )
        entries: list[Mapping[str, object]] = []
        with self._engine.begin() as conn:
            for (payload,) in conn.execute(stmt).all():
                try:
                    record = json.loads(payload)
                except json.JSONDecodeError:
                    continue
                if isinstance(record, dict):
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
