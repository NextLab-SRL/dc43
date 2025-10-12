"""Delta-backed helpers for reading governance verdicts."""

from __future__ import annotations

import json
from typing import Any, Mapping

from dc43_service_clients.data_quality import ValidationResult, coerce_details


class DeltaGovernanceStatusStore:
    """Persist and load governance verdicts stored in Delta tables."""

    def __init__(self, spark: Any, *, table: str | None = None, path: str | None = None) -> None:
        if not (table or path):  # pragma: no cover - defensive guard
            raise ValueError("Provide a Unity Catalog table name or Delta path")
        self._spark = spark
        self._table = table
        self._path = path

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------
    def _table_ref(self) -> str:
        return self._table if self._table else f"delta.`{self._path}`"

    def _select_status_query(self, dataset_id: str, dataset_version: str) -> str:
        ref = self._table_ref()
        safe_dataset = dataset_id.replace("'", "''")
        safe_version = dataset_version.replace("'", "''")
        return (
            "SELECT status, reason, details, metrics, schema "
            f"FROM {ref} "
            f"WHERE dataset_id = '{safe_dataset}' AND dataset_version = '{safe_version}' "
            "ORDER BY recorded_at DESC LIMIT 1"
        )

    @staticmethod
    def _decode_details(raw: object) -> Mapping[str, Any]:
        if raw is None:
            return {}
        if isinstance(raw, str):
            try:
                payload = json.loads(raw)
            except json.JSONDecodeError:
                return {}
            if isinstance(payload, Mapping):
                return dict(payload)
            return coerce_details(payload)
        return coerce_details(raw)

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------
    def load_status(self, *, dataset_id: str, dataset_version: str) -> ValidationResult:
        """Return the latest governance verdict recorded for ``dataset_id``."""

        query = self._select_status_query(dataset_id, dataset_version)
        rows = self._spark.sql(query).head(1)
        if not rows:
            raise LookupError(f"No governance status for {dataset_id}:{dataset_version}")

        row = rows[0]
        status = getattr(row, "status", "unknown")
        reason = getattr(row, "reason", None)
        details_payload = self._decode_details(getattr(row, "details", None))
        result = ValidationResult(status=status, reason=reason, details=details_payload)

        metrics_payload = getattr(row, "metrics", None)
        if isinstance(metrics_payload, Mapping):
            result.metrics = dict(metrics_payload)
        schema_payload = getattr(row, "schema", None)
        if isinstance(schema_payload, Mapping):
            result.schema = {key: dict(value) for key, value in schema_payload.items()}

        return result


__all__ = ["DeltaGovernanceStatusStore"]
