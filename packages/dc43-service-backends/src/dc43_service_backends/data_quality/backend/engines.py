"""Pluggable execution engines for the data-quality manager."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Mapping, Optional, Protocol, Sequence

from open_data_contract_standard.model import OpenDataContractStandard  # type: ignore

from dc43_service_backends.data_quality.backend.engine import (
    ValidationResult,
    evaluate_contract,
)
from dc43_service_backends.data_quality.backend.predicates import (
    expectation_plan,
    expectation_predicates_from_plan,
)
from dc43_service_clients.data_quality import ObservationPayload

try:  # pragma: no cover - optional dependency
    import yaml  # type: ignore
except ModuleNotFoundError:  # pragma: no cover - optional dependency
    yaml = None  # type: ignore[assignment]


class DataQualityExecutionEngine(Protocol):
    """Execution contract implemented by concrete data-quality engines."""

    def evaluate(
        self,
        contract: OpenDataContractStandard,
        payload: ObservationPayload,
    ) -> ValidationResult:
        """Return the validation outcome for ``contract`` and ``payload``."""

    def describe_expectations(
        self,
        contract: OpenDataContractStandard,
    ) -> Sequence[Mapping[str, object]]:
        """Describe expectations that the engine will enforce."""


class NativeDataQualityEngine(DataQualityExecutionEngine):
    """Adapter around the built-in contract evaluation engine."""

    def __init__(
        self,
        *,
        strict_types: bool = True,
        allow_extra_columns: bool = True,
        expectation_severity: str = "error",
    ) -> None:
        self._strict_types = strict_types
        self._allow_extra_columns = allow_extra_columns
        self._expectation_severity = expectation_severity

    def evaluate(
        self,
        contract: OpenDataContractStandard,
        payload: ObservationPayload,
    ) -> ValidationResult:
        result = evaluate_contract(
            contract,
            schema=payload.schema,
            metrics=payload.metrics,
            strict_types=self._strict_types,
            allow_extra_columns=self._allow_extra_columns,
            expectation_severity=self._expectation_severity,  # type: ignore[arg-type]
        )
        plan = expectation_plan(contract)
        updates: dict[str, object] = {}
        if plan:
            updates["expectation_plan"] = plan
            predicates = expectation_predicates_from_plan(plan)
            if predicates:
                updates["expectation_predicates"] = predicates
        if updates:
            result.merge_details(updates)
        return result

    def describe_expectations(
        self,
        contract: OpenDataContractStandard,
    ) -> Sequence[Mapping[str, object]]:
        return expectation_plan(contract)


class _SuiteLoader:
    """Utility helper to load expectation suites for external engines."""

    def __init__(self, path: str | Path | None) -> None:
        self._path = Path(path).expanduser() if path else None

    def load(self) -> Sequence[Mapping[str, object]]:
        if self._path is None or not self._path.exists():
            return []
        try:
            data = self._path.read_text("utf-8")
        except OSError:
            return []
        if self._path.suffix.lower() in {".json", ".ge"}:
            try:
                payload = json.loads(data)
            except json.JSONDecodeError:
                return []
        elif self._path.suffix.lower() in {".yaml", ".yml"} and yaml is not None:
            try:
                payload = yaml.safe_load(data)  # type: ignore[assignment]
            except Exception:
                return []
        else:
            return []
        if isinstance(payload, Mapping):
            return [dict(payload)]
        if isinstance(payload, list):
            return [item for item in payload if isinstance(item, Mapping)]
        return []


class MetricsDrivenEngine(DataQualityExecutionEngine):
    """Base class for engines that interpret metrics emitted by external tools."""

    def __init__(self, *, metrics_key: str, suite_path: str | Path | None = None) -> None:
        self._metrics_key = metrics_key
        self._suite_loader = _SuiteLoader(suite_path)

    def _metrics_summary(self, payload: ObservationPayload) -> Mapping[str, object] | None:
        metrics = payload.metrics or {}
        summary = metrics.get(self._metrics_key)
        if isinstance(summary, Mapping):
            return summary
        return None

    def describe_expectations(
        self,
        contract: OpenDataContractStandard,
    ) -> Sequence[Mapping[str, object]]:
        suite = self._suite_loader.load()
        if suite:
            return suite
        return expectation_plan(contract)


class GreatExpectationsEngine(MetricsDrivenEngine):
    """Translate Great Expectations run summaries into validation results."""

    def __init__(
        self,
        *,
        metrics_key: str = "great_expectations",
        suite_path: str | Path | None = None,
    ) -> None:
        super().__init__(metrics_key=metrics_key, suite_path=suite_path)

    def evaluate(
        self,
        contract: OpenDataContractStandard,
        payload: ObservationPayload,
    ) -> ValidationResult:
        summary = self._metrics_summary(payload)
        if not summary:
            return ValidationResult(
                status="unknown",
                reason="missing-great-expectations-summary",
                details={"engine": "great_expectations", "metrics_key": self._metrics_key},
            )
        status = str(summary.get("status") or ("ok" if summary.get("success") else "block"))
        if status not in {"ok", "warn", "block", "unknown"}:
            status = "ok" if summary.get("success") else "block"
        reason = summary.get("exception_info") or summary.get("reason")
        if isinstance(reason, Mapping):
            reason = reason.get("exception_message")
        return ValidationResult(
            status=status,
            reason=str(reason) if reason else None,
            details=dict(summary),
        )


class SodaEngine(MetricsDrivenEngine):
    """Interpret Soda scan outcomes from observation metrics."""

    def __init__(
        self,
        *,
        metrics_key: str = "soda",
        checks_path: str | Path | None = None,
    ) -> None:
        super().__init__(metrics_key=metrics_key, suite_path=checks_path)

    def evaluate(
        self,
        contract: OpenDataContractStandard,
        payload: ObservationPayload,
    ) -> ValidationResult:
        summary = self._metrics_summary(payload)
        if not summary:
            return ValidationResult(
                status="unknown",
                reason="missing-soda-summary",
                details={"engine": "soda", "metrics_key": self._metrics_key},
            )
        outcome = str(summary.get("status") or summary.get("outcome") or "warn")
        if outcome not in {"ok", "warn", "block", "unknown"}:
            outcome = "ok" if str(summary.get("status", "")).lower() in {"passed", "pass"} else "block"
        reason = summary.get("reason") or summary.get("error")
        return ValidationResult(
            status=outcome,
            reason=str(reason) if reason else None,
            details=dict(summary),
        )


__all__ = [
    "DataQualityExecutionEngine",
    "NativeDataQualityEngine",
    "GreatExpectationsEngine",
    "SodaEngine",
]
