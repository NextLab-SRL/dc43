"""In-memory implementation of :class:`GovernanceStore`."""

from __future__ import annotations

import json
from collections import defaultdict
from datetime import datetime, timezone
from typing import Dict, List, Mapping, MutableMapping, Optional, Sequence

from dc43_service_clients.data_quality import ValidationResult

from ._metrics import extract_metrics, normalise_metric_value
from .interface import GovernanceStore


class InMemoryGovernanceStore(GovernanceStore):
    """Store governance artefacts in process memory."""

    def __init__(self) -> None:
        self._status_cache: Dict[tuple[str, str, str, str], ValidationResult] = {}
        self._dataset_links: Dict[tuple[str, Optional[str]], str] = {}
        self._activity_log: Dict[str, MutableMapping[str, MutableMapping[str, object]]] = (
            defaultdict(dict)
        )
        self._metrics: List[Dict[str, object]] = []

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
        key = (contract_id, contract_version, dataset_id, dataset_version)
        if status is None:
            self._status_cache.pop(key, None)
            return

        recorded_at = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
        self._status_cache[key] = status
        metrics_map = extract_metrics(status)
        if metrics_map:
            self._record_metrics(
                contract_id=contract_id,
                contract_version=contract_version,
                dataset_id=dataset_id,
                dataset_version=dataset_version,
                recorded_at=recorded_at,
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
        return self._status_cache.get((contract_id, contract_version, dataset_id, dataset_version))

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
        link_value = f"{contract_id}:{contract_version}"
        self._dataset_links[(dataset_id, dataset_version)] = link_value
        if dataset_version:
            self._dataset_links.setdefault((dataset_id, None), link_value)

    def get_linked_contract_version(
        self,
        *,
        dataset_id: str,
        dataset_version: Optional[str] = None,
    ) -> str | None:
        if (dataset_id, dataset_version) in self._dataset_links:
            return self._dataset_links[(dataset_id, dataset_version)]
        if dataset_version is not None and (dataset_id, None) in self._dataset_links:
            return self._dataset_links[(dataset_id, None)]
        for (linked_id, linked_version), value in self._dataset_links.items():
            if linked_id != dataset_id:
                continue
            if dataset_version is None or linked_version == dataset_version:
                return value
        return None

    # ------------------------------------------------------------------
    # Metrics
    # ------------------------------------------------------------------
    def _record_metrics(
        self,
        *,
        contract_id: str,
        contract_version: str,
        dataset_id: str,
        dataset_version: str,
        recorded_at: str,
        metrics: Mapping[str, object],
    ) -> None:
        for metric_key, metric_value in metrics.items():
            value, numeric = normalise_metric_value(metric_value)
            self._metrics.append(
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

    def load_metrics(
        self,
        *,
        dataset_id: str,
        dataset_version: Optional[str] = None,
        contract_id: Optional[str] = None,
        contract_version: Optional[str] = None,
    ) -> Sequence[Mapping[str, object]]:
        records: list[Dict[str, object]] = []
        for entry in self._metrics:
            if entry.get("dataset_id") != dataset_id:
                continue
            if dataset_version is not None and entry.get("dataset_version") != dataset_version:
                continue
            if contract_id is not None and entry.get("contract_id") != contract_id:
                continue
            if contract_version is not None and entry.get("contract_version") != contract_version:
                continue
            records.append(dict(entry))

        records.sort(
            key=lambda item: (
                str(item.get("status_recorded_at", "")),
                str(item.get("metric_key", "")),
            )
        )
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
        recorded_at = str(
            event.get("recorded_at")
            or datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
        )
        entry: Dict[str, object] = {
            "dataset_id": dataset_id,
            "dataset_version": dataset_version,
            "contract_id": contract_id,
            "contract_version": contract_version,
            "events": [],
        }
        record = self._activity_log[dataset_id].get(dataset_version)
        if isinstance(record, MutableMapping):
            entry.update(record)

        events = list(entry.get("events") or [])
        payload = dict(event)
        payload.setdefault("recorded_at", recorded_at)
        events.append(payload)
        entry["events"] = events
        if lineage_event is not None:
            entry["lineage_event"] = dict(lineage_event)
        entry["contract_id"] = contract_id
        entry["contract_version"] = contract_version
        entry["dataset_id"] = dataset_id
        entry["dataset_version"] = dataset_version
        self._activity_log[dataset_id][dataset_version] = entry

    def list_datasets(self) -> Sequence[str]:
        dataset_ids = [dataset_id for dataset_id in self._activity_log.keys()]
        dataset_ids.sort()
        return dataset_ids

    def load_pipeline_activity(
        self,
        *,
        dataset_id: str,
        dataset_version: Optional[str] = None,
    ) -> Sequence[Mapping[str, object]]:
        dataset_entries = self._activity_log.get(dataset_id)
        if not dataset_entries:
            return []

        def _normalise(record: Mapping[str, object]) -> Dict[str, object]:
            entry = {
                "dataset_id": dataset_id,
                "dataset_version": record.get("dataset_version"),
                "contract_id": record.get("contract_id"),
                "contract_version": record.get("contract_version"),
                "events": [],
            }
            events = record.get("events")
            if isinstance(events, list):
                entry["events"] = [
                    dict(event)
                    for event in events
                    if isinstance(event, Mapping)
                ]
            lineage_payload = record.get("lineage_event")
            if isinstance(lineage_payload, Mapping):
                entry["lineage_event"] = dict(lineage_payload)
            return entry

        if dataset_version is not None:
            record = dataset_entries.get(dataset_version)
            if isinstance(record, Mapping):
                return [_normalise(record)]
            for candidate in dataset_entries.values():
                if (
                    isinstance(candidate, Mapping)
                    and candidate.get("dataset_version") == dataset_version
                ):
                    return [_normalise(candidate)]
            return []

        entries = [_normalise(record) for record in dataset_entries.values() if isinstance(record, Mapping)]

        def _sort_key(item: Mapping[str, object]) -> tuple[int, str]:
            events = item.get("events")
            recorded_at = ""
            if isinstance(events, list) and events:
                last = events[-1]
                if isinstance(last, Mapping):
                    recorded_at = str(last.get("recorded_at", ""))
            version_value = str(item.get("dataset_version", ""))
            return (0 if recorded_at else 1, recorded_at or version_value)

        entries.sort(key=_sort_key)
        return entries


__all__ = ["InMemoryGovernanceStore"]
