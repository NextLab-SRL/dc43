"""Filesystem-backed stub for governance-facing data-quality clients."""

from __future__ import annotations

import json
import logging
import os
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Mapping, Sequence

from dc43_service_backends.contracts.drafting import draft_from_validation_result
from dc43_service_backends.data_quality.backend.engine import evaluate_contract
from dc43_service_clients.data_quality import ValidationResult, coerce_details
from dc43_service_backends.core.odcs import contract_identity
from open_data_contract_standard.model import OpenDataContractStandard  # type: ignore

logger = logging.getLogger(__name__)


class StubDQClient:
    """Filesystem-backed stub for a DQ/DO service."""

    def __init__(self, base_path: str, *, block_on_violation: bool = True):
        self.base_path = base_path.rstrip("/")
        self.block_on_violation = block_on_violation
        logger.info("Initialized StubDQClient at %s", self.base_path)

    def _safe(self, s: str) -> str:
        """Return a filesystem-safe version of ``s``."""

        return "".join(ch if ch.isalnum() or ch in ("_", "-", ".") else "_" for ch in s)

    def _links_dir(self) -> str:
        d = os.path.join(self.base_path, "links")
        os.makedirs(d, exist_ok=True)
        return d

    def _links_path(self, dataset_id: str) -> str:
        return os.path.join(self._links_dir(), f"{self._safe(dataset_id)}.json")

    def _load_links(self, dataset_id: str) -> Dict[str, Any]:
        path = self._links_path(dataset_id)
        if not os.path.exists(path):
            return {"versions": {}}
        try:
            with open(path, "r", encoding="utf-8") as f:
                data = json.load(f)
        except (OSError, json.JSONDecodeError):
            return {"versions": {}}

        if not isinstance(data, dict):
            return {"versions": {}}

        # Upgrade legacy payloads that stored a single entry at the root.
        if "versions" not in data:
            entry = {
                key: data[key]
                for key in ("contract_id", "contract_version", "dataset_version", "linked_at")
                if key in data
            }
            upgraded: Dict[str, Any] = {"versions": {}}
            if entry.get("dataset_version"):
                upgraded["versions"][entry["dataset_version"]] = entry
                upgraded["latest"] = entry
            else:
                upgraded["latest"] = entry
            return upgraded

        versions = data.get("versions")
        if not isinstance(versions, dict):
            data["versions"] = {}
        return data

    def _status_path(self, dataset_id: str, dataset_version: str) -> str:
        d = os.path.join(self.base_path, "status", self._safe(dataset_id))
        os.makedirs(d, exist_ok=True)
        return os.path.join(d, f"{self._safe(str(dataset_version))}.json")

    def _activity_dir(self) -> str:
        d = os.path.join(self.base_path, "pipeline_activity")
        os.makedirs(d, exist_ok=True)
        return d

    def _activity_path(self, dataset_id: str) -> str:
        return os.path.join(self._activity_dir(), f"{self._safe(dataset_id)}.json")

    def _load_activity(self, dataset_id: str) -> Dict[str, Any]:
        path = self._activity_path(dataset_id)
        if not os.path.exists(path):
            return {"dataset_id": dataset_id, "versions": {}}
        try:
            with open(path, "r", encoding="utf-8") as f:
                payload = json.load(f)
        except (OSError, json.JSONDecodeError):
            return {"dataset_id": dataset_id, "versions": {}}

        if not isinstance(payload, dict):
            return {"dataset_id": dataset_id, "versions": {}}
        versions = payload.get("versions")
        if not isinstance(versions, dict):
            payload["versions"] = {}
        return payload

    def get_status(
        self,
        *,
        contract_id: str,
        contract_version: str,
        dataset_id: str,
        dataset_version: str,
    ) -> ValidationResult:
        path = self._status_path(dataset_id, dataset_version)
        logger.debug("Fetching DQ status from %s", path)
        if not os.path.exists(path):
            return ValidationResult(status="unknown", reason="no-status-for-version")
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        link = self.get_linked_contract_version(
            dataset_id=dataset_id,
            dataset_version=dataset_version,
        )
        if link and link != f"{contract_id}:{contract_version}":
            return ValidationResult(status="block", reason=f"dataset linked to contract {link}", details=data)
        return ValidationResult(status=data.get("status", "warn"), reason=data.get("reason"), details=data.get("details", {}))

    def submit_metrics(
        self,
        *,
        contract: OpenDataContractStandard,
        dataset_id: str,
        dataset_version: str,
        metrics: Dict[str, Any],
    ) -> ValidationResult:
        schema_payload = metrics.get("schema") if isinstance(metrics, dict) else None
        metric_values = metrics.copy() if isinstance(metrics, dict) else {}
        if "schema" in metric_values:
            metric_values = dict(metric_values)
            metric_values.pop("schema", None)

        evaluation = evaluate_contract(
            contract,
            schema=schema_payload if isinstance(schema_payload, Mapping) else None,
            metrics=metric_values,
            strict_types=True,
            allow_extra_columns=True,
            expectation_severity="error",
        )

        blocking = self.block_on_violation
        violations = 0
        for k, v in metric_values.items():
            if k.startswith("violations.") or k.startswith("query."):
                if isinstance(v, (int, float)):
                    violations += int(v)

        if evaluation.errors:
            status = "block" if blocking else "warn"
        elif evaluation.warnings and blocking:
            status = "warn"
        else:
            status = "ok"

        details = coerce_details(evaluation.details)
        if not details:
            details = {
                "errors": list(evaluation.errors),
                "warnings": list(evaluation.warnings),
                "metrics": dict(evaluation.metrics),
                "schema": dict(evaluation.schema),
            }
        details["violations"] = violations

        path = self._status_path(dataset_id, dataset_version)
        logger.info("Persisting DQ status %s for %s@%s to %s", status, dataset_id, dataset_version, path)
        contract_id_value, contract_version_value = contract_identity(contract)
        recorded_at = datetime.now(timezone.utc).isoformat()
        payload = {
            "status": status,
            "details": details,
            "dataset_id": dataset_id,
            "dataset_version": dataset_version,
            "contract_id": contract_id_value,
            "contract_version": contract_version_value,
            "recorded_at": recorded_at,
        }
        with open(path, "w", encoding="utf-8") as f:
            json.dump(payload, f)

        self.link_dataset_contract(
            dataset_id=dataset_id,
            dataset_version=dataset_version,
            contract_id=contract_id_value,
            contract_version=contract_version_value,
        )
        return ValidationResult(status=status, details=details)

    def load_status_matrix_entries(
        self,
        *,
        dataset_id: str,
        dataset_versions: Sequence[str] | None = None,
        contract_ids: Sequence[str] | None = None,
    ) -> Sequence[Mapping[str, object]]:
        versions = {str(value) for value in (dataset_versions or []) if str(value)}
        contracts = {str(value) for value in (contract_ids or []) if str(value)}
        status_dir = os.path.join(self.base_path, "status", self._safe(dataset_id))
        if not os.path.isdir(status_dir):
            return ()
        entries: list[Mapping[str, object]] = []
        for name in os.listdir(status_dir):
            path = os.path.join(status_dir, name)
            try:
                with open(path, "r", encoding="utf-8") as handle:
                    payload = json.load(handle)
            except (OSError, json.JSONDecodeError):
                continue
            dataset_version = str(payload.get("dataset_version") or "")
            if versions and dataset_version not in versions:
                continue
            contract_id = str(payload.get("contract_id") or "")
            if contracts and contract_id not in contracts:
                continue
            contract_version = str(payload.get("contract_version") or "")
            status = ValidationResult(
                status=str(payload.get("status", "unknown")),
                reason=str(payload.get("reason")) if payload.get("reason") else None,
                details=coerce_details(payload.get("details")),
            )
            entries.append(
                {
                    "dataset_id": dataset_id,
                    "dataset_version": dataset_version,
                    "contract_id": contract_id,
                    "contract_version": contract_version,
                    "status": status,
                }
            )
        return tuple(entries)

    def record_pipeline_activity(
        self,
        *,
        dataset_id: str,
        dataset_version: str,
        contract_id: Optional[str],
        contract_version: Optional[str],
        activity: Mapping[str, Any],
    ) -> None:
        payload = self._load_activity(dataset_id)
        versions = payload.setdefault("versions", {})
        entry = versions.get(dataset_version)
        if not isinstance(entry, dict):
            entry = {"dataset_version": dataset_version, "events": []}

        events: List[Dict[str, Any]]
        raw_events = entry.get("events")
        if isinstance(raw_events, list):
            events = [event for event in raw_events if isinstance(event, dict)]
        else:
            events = []

        event = dict(activity)
        event.setdefault("recorded_at", datetime.now(timezone.utc).isoformat())
        context_payload = event.get("pipeline_context")
        if isinstance(context_payload, Mapping):
            event["pipeline_context"] = dict(context_payload)
        elif context_payload is None:
            event["pipeline_context"] = {}

        events.append(event)

        entry["dataset_version"] = dataset_version
        entry["contract_id"] = contract_id
        entry["contract_version"] = contract_version
        entry["events"] = events
        versions[dataset_version] = entry
        payload["dataset_id"] = dataset_id
        payload["latest"] = entry

        path = self._activity_path(dataset_id)
        with open(path, "w", encoding="utf-8") as f:
            json.dump(payload, f)

    def get_pipeline_activity(
        self,
        *,
        dataset_id: str,
        dataset_version: Optional[str] = None,
        include_status: bool = False,
    ) -> List[Dict[str, Any]]:
        payload = self._load_activity(dataset_id)
        versions = payload.get("versions")
        if not isinstance(versions, Mapping):
            return []

        def _normalise_entry(key: str, value: Mapping[str, Any]) -> Dict[str, Any]:
            record = dict(value)
            record.setdefault("dataset_version", key)
            events = record.get("events")
            if not isinstance(events, list):
                record["events"] = []
            else:
                record["events"] = [event for event in events if isinstance(event, dict)]
            return record

        if dataset_version is not None:
            entry = versions.get(dataset_version)
            if isinstance(entry, Mapping):
                return [_normalise_entry(dataset_version, entry)]
            # Attempt to match unsanitised versions stored as values
            for candidate_version, candidate in versions.items():
                if (
                    isinstance(candidate, Mapping)
                    and candidate.get("dataset_version") == dataset_version
                ):
                    return [_normalise_entry(candidate_version, candidate)]
            return []

        records: List[Dict[str, Any]] = []
        for key, value in versions.items():
            if isinstance(value, Mapping):
                records.append(_normalise_entry(str(key), value))

        def _sort_key(entry: Mapping[str, Any]) -> tuple[int, str]:
            version_value = str(entry.get("dataset_version", ""))
            recorded_at = ""
            events = entry.get("events")
            if isinstance(events, list) and events:
                last = events[-1]
                if isinstance(last, Mapping):
                    recorded_at = str(last.get("recorded_at", ""))
            return (0 if recorded_at else 1, recorded_at or version_value)

        records.sort(key=_sort_key)
        return records

    def propose_draft(
        self,
        *,
        validation: ValidationResult,
        base_contract: OpenDataContractStandard,
        bump: str = "minor",
        dataset_id: Optional[str] = None,
        dataset_version: Optional[str] = None,
        data_format: Optional[str] = None,
        dq_feedback: Optional[Mapping[str, Any]] = None,
        draft_context: Optional[Mapping[str, Any]] = None,
    ) -> Optional[OpenDataContractStandard]:
        """Produce a draft contract using the engine helper."""

        return draft_from_validation_result(
            validation=validation,
            base_contract=base_contract,
            bump=bump,
            dataset_id=dataset_id,
            dataset_version=dataset_version,
            data_format=data_format,
            dq_feedback=dq_feedback,
            draft_context=draft_context,
        )

    def link_dataset_contract(
        self,
        *,
        dataset_id: str,
        dataset_version: str,
        contract_id: str,
        contract_version: str,
    ) -> None:
        path = self._links_path(dataset_id)
        logger.info(
            "Linking dataset %s@%s to contract %s:%s at %s",
            dataset_id,
            dataset_version,
            contract_id,
            contract_version,
            path,
        )
        payload = self._load_links(dataset_id)
        linked_at = datetime.now(timezone.utc).isoformat()
        entry = {
            "contract_id": contract_id,
            "contract_version": contract_version,
            "dataset_version": dataset_version,
            "linked_at": linked_at,
        }
        versions = payload.setdefault("versions", {})
        versions[dataset_version] = entry
        payload["latest"] = entry
        with open(path, "w", encoding="utf-8") as f:
            json.dump(payload, f)

    def get_linked_contract_version(
        self,
        *,
        dataset_id: str,
        dataset_version: Optional[str] = None,
    ) -> Optional[str]:
        payload = self._load_links(dataset_id)

        def _entry_to_link(entry: Mapping[str, Any] | None) -> Optional[str]:
            if not isinstance(entry, Mapping):
                return None
            contract_id_value = entry.get("contract_id")
            contract_version_value = entry.get("contract_version")
            if contract_id_value and contract_version_value:
                link_value = f"{contract_id_value}:{contract_version_value}"
                logger.debug("Found contract link for %s -> %s", dataset_id, link_value)
                return link_value
            return None

        versions = payload.get("versions") if isinstance(payload, Mapping) else None
        if dataset_version and isinstance(versions, Mapping):
            entry = versions.get(dataset_version)
            if entry is None:
                # Attempt to match on stored dataset version values when keys were sanitised.
                for candidate in versions.values():
                    if (
                        isinstance(candidate, Mapping)
                        and candidate.get("dataset_version") == dataset_version
                    ):
                        entry = candidate
                        break
            link = _entry_to_link(entry)
            if link:
                return link

        latest = payload.get("latest") if isinstance(payload, Mapping) else None
        link = _entry_to_link(latest)
        if link:
            return link

        # Compatibility with legacy payloads that stored the entry at the root.
        return _entry_to_link(payload if isinstance(payload, Mapping) else None)


__all__ = ["StubDQClient"]
