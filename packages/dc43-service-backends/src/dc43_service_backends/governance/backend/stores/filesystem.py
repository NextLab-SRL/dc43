"""Filesystem-backed governance persistence implementation."""

from __future__ import annotations

import json
import os
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping, Optional, Sequence

from dc43_service_clients.data_quality import ValidationResult, coerce_details

from ._metrics import extract_metrics, normalise_metric_value
from .interface import GovernanceStore


@dataclass(slots=True)
class _StatusRecord:
    contract_id: str
    contract_version: str
    dataset_id: str
    dataset_version: str
    status: str
    reason: str | None
    details: Mapping[str, object]
    recorded_at: str


class FilesystemGovernanceStore(GovernanceStore):
    """Persist governance artefacts to JSON files on disk."""

    def __init__(self, base_path: str | os.PathLike[str]) -> None:
        self.base_path = Path(base_path).expanduser()
        self.base_path.mkdir(parents=True, exist_ok=True)
        for subdir in ("status", "links", "pipeline_activity", "metrics"):
            (self.base_path / subdir).mkdir(parents=True, exist_ok=True)

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------
    def _safe(self, value: str) -> str:
        return "".join(ch if ch.isalnum() or ch in {"-", "_", "."} else "_" for ch in value)

    def _status_path(self, dataset_id: str, dataset_version: str) -> Path:
        folder = self.base_path / "status" / self._safe(dataset_id)
        folder.mkdir(parents=True, exist_ok=True)
        return folder / f"{self._safe(dataset_version)}.json"

    def _links_path(self, dataset_id: str) -> Path:
        folder = self.base_path / "links"
        folder.mkdir(parents=True, exist_ok=True)
        return folder / f"{self._safe(dataset_id)}.json"

    def _activity_path(self, dataset_id: str) -> Path:
        folder = self.base_path / "pipeline_activity"
        folder.mkdir(parents=True, exist_ok=True)
        return folder / f"{self._safe(dataset_id)}.json"

    def _metrics_path(self, dataset_id: str) -> Path:
        folder = self.base_path / "metrics"
        folder.mkdir(parents=True, exist_ok=True)
        return folder / f"{self._safe(dataset_id)}.json"

    def _read_json(self, path: Path) -> Mapping[str, object] | None:
        if not path.exists():
            return None
        try:
            payload = path.read_text("utf-8")
            data = json.loads(payload)
        except (OSError, json.JSONDecodeError):
            return None
        if isinstance(data, Mapping):
            return data
        return None

    def _write_json(self, path: Path, payload: Mapping[str, object]) -> None:
        tmp = path.with_suffix(".tmp")
        path.parent.mkdir(parents=True, exist_ok=True)
        tmp.write_text(json.dumps(payload, indent=2, sort_keys=True), "utf-8")
        tmp.replace(path)

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
        path = self._status_path(dataset_id, dataset_version)
        if status is None:
            try:
                path.unlink()
            except FileNotFoundError:
                return
            return

        record = _StatusRecord(
            contract_id=contract_id,
            contract_version=contract_version,
            dataset_id=dataset_id,
            dataset_version=dataset_version,
            status=status.status,
            reason=status.reason,
            details=status.details,
            recorded_at=datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        )
        self._write_json(path, asdict(record))

        metrics_map = extract_metrics(status)
        if metrics_map:
            self._append_metrics(
                contract_id=contract_id,
                contract_version=contract_version,
                dataset_id=dataset_id,
                dataset_version=dataset_version,
                recorded_at=record.recorded_at,
                metrics=metrics_map,
            )

    def load_status(
        self,
        *,
        contract_id: str,
        contract_version: str,
        dataset_id: str,
        dataset_version: str,
    ) -> ValidationResult | None:
        path = self._status_path(dataset_id, dataset_version)
        payload = self._read_json(path)
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
        path = self._links_path(dataset_id)
        payload = self._read_json(path) or {"versions": {}}
        versions = payload.get("versions")
        if not isinstance(versions, dict):
            versions = {}
        entry = {
            "contract_id": contract_id,
            "contract_version": contract_version,
            "dataset_version": dataset_version,
            "linked_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        }
        versions[str(dataset_version)] = entry
        payload["versions"] = versions
        payload["latest"] = entry
        self._write_json(path, payload)

    def get_linked_contract_version(
        self,
        *,
        dataset_id: str,
        dataset_version: Optional[str] = None,
    ) -> str | None:
        payload = self._read_json(self._links_path(dataset_id))
        if not payload:
            return None
        versions = payload.get("versions")
        if isinstance(versions, Mapping) and dataset_version is not None:
            entry = versions.get(str(dataset_version))
            if isinstance(entry, Mapping):
                cid = entry.get("contract_id")
                cver = entry.get("contract_version")
                if cid and cver:
                    return f"{cid}:{cver}"
        latest = payload.get("latest")
        if isinstance(latest, Mapping):
            cid = latest.get("contract_id")
            cver = latest.get("contract_version")
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
        path = self._activity_path(dataset_id)
        payload = self._read_json(path) or {"dataset_id": dataset_id, "versions": {}}
        versions = payload.get("versions")
        if not isinstance(versions, dict):
            versions = {}
        version_key = str(dataset_version)
        record = versions.get(version_key)
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
        versions[version_key] = record
        payload["versions"] = versions
        payload["updated_at"] = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
        self._write_json(path, payload)

    def list_datasets(self) -> Sequence[str]:
        datasets: list[str] = []
        activity_dir = self._activity_dir()
        if not activity_dir.exists():
            return datasets
        for entry in activity_dir.iterdir():
            if not entry.is_file() or entry.suffix != ".json":
                continue
            payload = self._read_json(entry)
            if not isinstance(payload, Mapping):
                continue
            dataset_id = payload.get("dataset_id")
            if isinstance(dataset_id, str) and dataset_id:
                datasets.append(dataset_id)
        datasets.sort()
        return datasets

    def load_pipeline_activity(
        self,
        *,
        dataset_id: str,
        dataset_version: Optional[str] = None,
    ) -> Sequence[Mapping[str, object]]:
        payload = self._read_json(self._activity_path(dataset_id))
        if not payload:
            return []
        versions = payload.get("versions")
        if not isinstance(versions, Mapping):
            return []
        if dataset_version is not None:
            record = versions.get(str(dataset_version))
            if isinstance(record, Mapping):
                return [dict(record)]
            return []
        entries: list[Mapping[str, object]] = []
        for record in versions.values():
            if isinstance(record, Mapping):
                entries.append(dict(record))
        entries.sort(
            key=lambda item: (
                0,
                str(
                    (item.get("events") or [{}])[-1].get("recorded_at", "")
                    if isinstance(item.get("events"), list) and item["events"]
                    else ""
                ),
            )
        )
        return entries

    # ------------------------------------------------------------------
    # Metric entries
    # ------------------------------------------------------------------
    def _append_metrics(
        self,
        *,
        contract_id: str,
        contract_version: str,
        dataset_id: str,
        dataset_version: str,
        recorded_at: str,
        metrics: Mapping[str, Any],
    ) -> None:
        path = self._metrics_path(dataset_id)
        payload = self._read_json(path) or {"dataset_id": dataset_id, "versions": {}}
        versions = payload.get("versions")
        if not isinstance(versions, dict):
            versions = {}
        version_key = str(dataset_version)
        records = versions.get(version_key)
        if not isinstance(records, list):
            records = []

        for metric_key, metric_value in metrics.items():
            value, numeric = normalise_metric_value(metric_value)
            records.append(
                {
                    "dataset_id": dataset_id,
                    "dataset_version": dataset_version,
                    "contract_id": contract_id,
                    "contract_version": contract_version,
                    "status_recorded_at": recorded_at,
                    "metric_key": str(metric_key),
                    "metric_value": value,
                    "metric_numeric_value": numeric,
                }
            )

        versions[version_key] = records
        payload["versions"] = versions
        self._write_json(path, payload)

    def load_metrics(
        self,
        *,
        dataset_id: str,
        dataset_version: Optional[str] = None,
        contract_id: Optional[str] = None,
        contract_version: Optional[str] = None,
    ) -> Sequence[Mapping[str, object]]:
        payload = self._read_json(self._metrics_path(dataset_id))
        if not payload:
            return []
        versions = payload.get("versions")
        if not isinstance(versions, Mapping):
            return []

        def _matches(entry: Mapping[str, object]) -> bool:
            if dataset_version is not None and entry.get("dataset_version") != dataset_version:
                return False
            if contract_id is not None and entry.get("contract_id") != contract_id:
                return False
            if contract_version is not None and entry.get("contract_version") != contract_version:
                return False
            return True

        records: list[Mapping[str, object]] = []
        for record_list in versions.values():
            if isinstance(record_list, list):
                for entry in record_list:
                    if isinstance(entry, Mapping) and _matches(entry):
                        records.append(dict(entry))

        records.sort(
            key=lambda item: (
                str(item.get("status_recorded_at", "")),
                str(item.get("metric_key", "")),
            )
        )
        return records


__all__ = ["FilesystemGovernanceStore"]
