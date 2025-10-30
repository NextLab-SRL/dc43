"""Structured Streaming scenarios integrated into the demo application."""
from __future__ import annotations

import json
import logging
import os
import shutil
import time
from collections.abc import Iterable as IterableABC
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Mapping, MutableMapping, Optional, Protocol

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.streaming import StreamingQuery, StreamingQueryException

from dc43_integrations.spark.io import (
    StaticDatasetLocator,
    GovernanceSparkReadRequest,
    GovernanceSparkWriteRequest,
    read_stream_with_governance,
    write_stream_with_governance,
)
from dc43_service_clients.governance import GovernanceReadContext, GovernanceWriteContext
from dc43_service_clients.data_quality import ValidationResult

from .contracts_api import (
    DatasetRecord,
    contract_service,
    dq_service,
    governance_service,
    refresh_dataset_aliases,
    register_dataset_version,
)
from .contracts_workspace import current_workspace


logger = logging.getLogger(__name__)


_INPUT_CONTRACT = "demo.streaming.events"
_OUTPUT_CONTRACT = "demo.streaming.events_processed"
_REJECT_DATASET = "demo.streaming.events_rejects"
_CONTRACT_VERSIONS: Dict[str, str] = {
    _INPUT_CONTRACT: "0.1.0",
    _OUTPUT_CONTRACT: "0.1.0",
}


class StreamingProgressReporter(Protocol):
    """Minimal protocol for streaming scenario progress emitters."""

    def emit(self, event: Mapping[str, Any]) -> None:
        """Publish a progress ``event``."""


@dataclass(slots=True)
class _RelatedDataset:
    """Supplementary dataset captured alongside the primary scenario result."""

    contract_id: str
    contract_version: str
    dataset_name: str
    dataset_version: Optional[str]
    status: str
    dq_details: Mapping[str, Any]
    run_type: str
    violations: int = 0
    reason: Optional[str] = None


@dataclass(slots=True)
class _ScenarioResult:
    """Container describing the outcome of a streaming scenario."""

    dataset_name: str
    dataset_version: Optional[str]
    validation: Optional[ValidationResult]
    queries: List[StreamingQuery]
    dq_details: Mapping[str, Any]
    timeline: List[Mapping[str, Any]]
    status_reason: Optional[str] = None
    related: List[_RelatedDataset] | None = None


def _spark_session() -> SparkSession:
    """Return the shared Spark session for streaming demos."""

    spark = SparkSession.getActiveSession()
    if spark is not None:
        try:
            spark.conf.set("spark.sql.adaptive.enabled", "false")
        except Exception:  # pragma: no cover - defensive configuration
            logger.exception("Failed to disable adaptive execution on shared session")
        spark.sparkContext.setLogLevel("WARN")
        return spark
    spark = (
        SparkSession.builder.master("local[2]")
        .appName("dc43-demo-streaming")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.sql.adaptive.enabled", "false")
        .getOrCreate()
    )
    try:
        spark.conf.set("spark.sql.adaptive.enabled", "false")
    except Exception:  # pragma: no cover - defensive configuration
        logger.exception("Failed to disable adaptive execution on new session")
    spark.sparkContext.setLogLevel("WARN")
    return spark


def _checkpoint_dir(name: str, *, version: str) -> Path:
    workspace = current_workspace()
    root = workspace.root / "streaming_checkpoints" / name / version
    root.mkdir(parents=True, exist_ok=True)
    return root


def _dataset_version_paths(dataset: str, version: str) -> tuple[Path, Path]:
    """Return ``(preferred, safe)`` paths for ``dataset`` and ``version``."""

    workspace = current_workspace()
    root = workspace.data_dir / dataset
    preferred = root / version
    safe_name = "".join(
        ch if ch.isalnum() or ch in {"-", "_", "."} else "_" for ch in version
    )
    if not safe_name:
        safe_name = "version"
    safe = root / safe_name
    return preferred, safe


def _write_version_marker(target: Path, version: str) -> None:
    """Persist a ``.dc43_version`` marker in ``target`` when possible."""

    marker = target / ".dc43_version"
    try:
        marker.write_text(version, encoding="utf-8")
    except OSError:  # pragma: no cover - best-effort marker creation
        logger.exception("Failed to record version marker for %s", target)


def _alias_dataset_version(preferred: Path, target: Path) -> None:
    """Create a symlink at ``preferred`` pointing to ``target`` if possible."""

    if os.name == "nt":  # pragma: no cover - Windows lacks ``:`` support in paths
        invalid = {":", "<", ">", "\"", "|", "?", "*"}
        if any(ch in preferred.name for ch in invalid):
            return
    try:
        preferred.parent.mkdir(parents=True, exist_ok=True)
        if preferred.is_symlink():
            preferred.unlink()
        elif preferred.exists():
            if preferred.is_dir():
                shutil.rmtree(preferred, ignore_errors=True)
            else:
                preferred.unlink()
        relative = os.path.relpath(target, preferred.parent)
        preferred.symlink_to(relative, target_is_directory=target.is_dir())
    except Exception:  # pragma: no cover - alias creation is best-effort
        logger.exception(
            "Failed to alias dataset version path %s -> %s", preferred, target
        )


def _ensure_streaming_version(dataset: str | None, version: Optional[str]) -> None:
    """Register ``version`` for ``dataset`` if possible."""

    if not dataset or not version:
        return
    try:
        preferred, safe = _dataset_version_paths(dataset, version)
        target = preferred if preferred.exists() else safe
        target.mkdir(parents=True, exist_ok=True)
        if target == safe and preferred != safe and not preferred.exists():
            _alias_dataset_version(preferred, safe)
    except Exception:  # pragma: no cover - defensive directory creation
        logger.exception("Failed to prepare dataset directory for %s %s", dataset, version)
        return
    try:
        register_dataset_version(dataset, version, target)
    except Exception:  # pragma: no cover - registration is best-effort for demo data
        logger.exception("Failed to register dataset version for %s %s", dataset, version)
        return
    refresh_dataset_aliases(dataset)


def _progress_emit(
    progress: Optional[StreamingProgressReporter],
    event: Mapping[str, Any],
) -> None:
    if progress is None:
        return
    try:
        progress.emit(dict(event))
    except Exception:  # pragma: no cover - defensive progress reporting
        logger.exception("Failed to emit streaming progress event")


def _is_streaming_shutdown_error(exc: BaseException | None) -> bool:
    """Return ``True`` when ``exc`` indicates a graceful streaming shutdown."""

    if exc is None:
        return False
    stack: List[BaseException] = [exc]
    seen: set[int] = set()
    while stack:
        current = stack.pop()
        identifier = id(current)
        if identifier in seen:
            continue
        seen.add(identifier)
        text = str(current) or repr(current)
        lowered = text.lower()
        if "interruptedexception" in lowered or "interruptedioexception" in lowered:
            return True
        nested = getattr(current, "__cause__", None)
        if isinstance(nested, BaseException):
            stack.append(nested)
        nested = getattr(current, "__context__", None)
        if isinstance(nested, BaseException):
            stack.append(nested)
        nested = getattr(current, "cause", None)
        if isinstance(nested, BaseException):
            stack.append(nested)
        java_exc = getattr(current, "java_exception", None)
        if java_exc is not None:
            try:
                java_text = str(java_exc).lower()
            except Exception:  # pragma: no cover - defensive conversion
                java_text = ""
            if "interruptedexception" in java_text or "interruptedioexception" in java_text:
                return True
    return False


def _drive_queries(queries: Iterable[StreamingQuery], *, seconds: int) -> None:
    """Advance ``queries`` for roughly ``seconds`` seconds."""

    active_queries = list(queries)
    if not active_queries:
        return

    def _query_num_rows(query: StreamingQuery) -> int:
        """Return the most recent ``numInputRows`` reported by ``query``."""

        try:
            progress = query.lastProgress
        except Exception:  # pragma: no cover - streaming engine specific failures
            return 0
        payload: Mapping[str, Any] | None = None
        if isinstance(progress, Mapping):
            payload = progress
        elif isinstance(progress, str):
            try:
                candidate = json.loads(progress)
            except Exception:
                candidate = None
            if isinstance(candidate, Mapping):
                payload = candidate
        if not payload:
            return 0
        value = payload.get("numInputRows")
        if isinstance(value, (int, float)):
            return int(value)
        if isinstance(value, str) and value.isdigit():
            return int(value)
        return 0

    # Ensure at least one micro-batch is processed even for short runs so that
    # metrics and validation details have a chance to update.
    remaining: List[StreamingQuery] = []
    seen_rows = False
    for query in active_queries:
        try:
            query.processAllAvailable()
        except StreamingQueryException as exc:
            if _is_streaming_shutdown_error(exc):
                logger.debug("Streaming query stopped while draining: %s", exc)
                continue
            raise
        except Exception as exc:  # pragma: no cover - streaming engines can close abruptly
            if _is_streaming_shutdown_error(exc):
                logger.debug("Streaming query interrupted while draining: %s", exc)
                continue
            logger.exception("Streaming query failed while draining batches")
            continue
        if _query_num_rows(query) > 0:
            seen_rows = True
        remaining.append(query)
    active_queries = remaining
    if not active_queries:
        return

    # Allow a short drain window so rate sources have time to emit at least one
    # populated micro-batch even when ``seconds`` is configured as ``0`` for
    # quick smoke tests.
    minimum_window = 0.5
    now = time.time()
    deadline = now + max(seconds, 0)
    drain_until = max(deadline, now + minimum_window)
    # Provide an extended window when no rows have been observed yet so the
    # initial micro-batch has time to populate.
    max_wait = max(drain_until, now + max(3.0, minimum_window + max(seconds, 0)))
    while active_queries and (
        time.time() < drain_until or (not seen_rows and time.time() < max_wait)
    ):
        current: List[StreamingQuery] = []
        for query in active_queries:
            try:
                query.processAllAvailable()
            except StreamingQueryException as exc:
                if _is_streaming_shutdown_error(exc):
                    logger.debug("Streaming query stopped during drain: %s", exc)
                    continue
                # Propagate failures to the caller after stopping all queries so the
                # dataset record can capture the reason from the validation payload.
                raise
            except Exception as exc:  # pragma: no cover - tolerate interrupted queries
                if _is_streaming_shutdown_error(exc):
                    logger.debug("Streaming query interrupted during drain: %s", exc)
                    continue
                logger.exception("Streaming query interrupted during drain")
                continue
            if not seen_rows and _query_num_rows(query) > 0:
                seen_rows = True
            current.append(query)
        active_queries = current
        if not active_queries:
            break
        if not seen_rows:
            drain_until = max(drain_until, time.time() + minimum_window)
        time.sleep(0.2)


def _stop_queries(queries: Iterable[StreamingQuery]) -> None:
    """Stop ``queries`` while giving metrics observers time to flush."""

    active: List[StreamingQuery] = []
    for query in queries:
        if query is None:
            continue
        active.append(query)
    if not active:
        return

    metrics_prefix = "dc43_metrics_"
    metrics_queries: List[StreamingQuery] = []
    other_queries: List[StreamingQuery] = []
    for query in active:
        name = getattr(query, "name", "") or ""
        if isinstance(name, str) and name.startswith(metrics_prefix):
            metrics_queries.append(query)
        else:
            other_queries.append(query)

    def _stop(query: StreamingQuery) -> None:
        try:
            if not query.isActive:
                return
            query.stop()
            try:
                query.awaitTermination(5)
            except TypeError:
                # Older PySpark signatures omit the timeout parameter.
                query.awaitTermination()
            except Exception:
                # Best-effort termination; ignore failures from Spark shutting down.
                pass
        except Exception:  # pragma: no cover - defensive shutdown
            logger.exception("Failed to stop streaming query %s", getattr(query, "name", ""))

    for query in metrics_queries:
        _stop(query)
    for query in other_queries:
        _stop(query)


def _query_metadata(queries: Iterable[StreamingQuery]) -> List[Mapping[str, Any]]:
    """Return serialisable metadata describing ``queries``."""

    info: List[Mapping[str, Any]] = []
    for query in queries:
        try:
            status = query.status
        except Exception:
            status = {}
        info.append(
            {
                "id": getattr(query, "id", ""),
                "name": getattr(query, "name", ""),
                "isActive": getattr(query, "isActive", False),
                "recentProgress": status.get("recentProgress", []),
                "message": status.get("message"),
            }
        )
    return info


def _extract_query_handles(details: Mapping[str, Any] | None) -> List[StreamingQuery]:
    """Return any :class:`~pyspark.sql.streaming.StreamingQuery` handles in ``details``."""

    if not isinstance(details, Mapping):
        return []
    handles: List[StreamingQuery] = []
    raw = details.get("streaming_queries")
    if isinstance(raw, StreamingQuery):
        handles.append(raw)
        return handles
    if isinstance(raw, IterableABC) and not isinstance(raw, (str, bytes)):
        for item in raw:
            if isinstance(item, StreamingQuery):
                handles.append(item)
    return handles


def _serialise_streaming_details(
    details: Mapping[str, Any] | None,
    *,
    queries: Iterable[StreamingQuery] | None = None,
) -> Dict[str, Any]:
    """Return a shallow copy of ``details`` with serialisable streaming metadata."""

    payload: Dict[str, Any] = _sanitize_validation_details(details)
    metadata: List[Mapping[str, Any]] = []
    handles = list(queries or [])
    if not handles:
        raw = payload.get("streaming_queries")
        if isinstance(raw, StreamingQuery):
            handles.append(raw)
        elif isinstance(raw, IterableABC) and not isinstance(raw, (str, bytes)):
            for item in raw:
                if isinstance(item, StreamingQuery):
                    handles.append(item)
                elif isinstance(item, Mapping):
                    metadata.append(dict(item))
        elif isinstance(raw, Mapping):
            metadata.append(dict(raw))
    if handles:
        metadata.extend(_query_metadata(handles))
    if metadata or "streaming_queries" in payload:
        payload["streaming_queries"] = metadata
    return payload


def _is_metric_warning(message: Any) -> bool:
    """Return ``True`` when ``message`` represents a missing metric warning."""

    if not isinstance(message, str):
        return False
    lowered = message.lower()
    return "violation counts were not provided" in lowered or lowered.startswith(
        "missing metric for expectation"
    )


def _sanitize_validation_details(
    details: Mapping[str, Any] | None,
) -> Dict[str, Any]:
    """Return a copy of ``details`` without noisy metric warnings."""

    payload: Dict[str, Any] = dict(details or {})
    warnings = payload.get("warnings")
    if isinstance(warnings, list):
        filtered = [w for w in warnings if not _is_metric_warning(w)]
        if len(filtered) != len(warnings):
            payload["warnings"] = filtered
    inner_details = payload.get("details")
    if isinstance(inner_details, Mapping):
        cleaned_inner = dict(inner_details)
        inner_warnings = cleaned_inner.get("warnings")
        if isinstance(inner_warnings, list):
            filtered_inner = [w for w in inner_warnings if not _is_metric_warning(w)]
            if len(filtered_inner) != len(inner_warnings):
                cleaned_inner["warnings"] = filtered_inner
                payload["details"] = cleaned_inner
    return payload


def _normalise_status(validation: Optional[ValidationResult]) -> str:
    if validation is None:
        return "error"
    status = (validation.status or "").lower()
    if status in {"warn", "warning"}:
        return "warning"
    if status in {"block", "error"}:
        return "error"
    if validation.ok and _extract_violation_count(validation.details) > 0:
        return "warning"
    if validation.errors:
        return "error"
    warnings = [
        warning
        for warning in validation.warnings
        if not _is_metric_warning(warning)
    ]
    if warnings:
        return "warning"
    return "ok"


def _extract_violation_count(section: Mapping[str, Any] | None) -> int:
    if not isinstance(section, Mapping):
        return 0
    total = 0
    candidate = section.get("violations")
    if isinstance(candidate, (int, float)):
        total = max(total, int(candidate))
    metrics = section.get("metrics")
    if isinstance(metrics, Mapping):
        for key, value in metrics.items():
            if str(key).startswith("violations") and isinstance(value, (int, float)):
                total = max(total, int(value))
    failed = section.get("failed_expectations")
    if isinstance(failed, Mapping):
        for info in failed.values():
            if isinstance(info, Mapping):
                count = info.get("count")
                if isinstance(count, (int, float)):
                    total = max(total, int(count))
    errors = section.get("errors")
    if isinstance(errors, list):
        total = max(total, len(errors))
    details = section.get("details")
    if isinstance(details, Mapping):
        total = max(total, _extract_violation_count(details))
    dq_status = section.get("dq_status")
    if isinstance(dq_status, Mapping):
        total = max(total, _extract_violation_count(dq_status))
    return total


def _timeline_event(
    *,
    phase: str,
    title: str,
    description: str,
    time_label: Optional[str] = None,
    status: str = "info",
    metrics: Mapping[str, Any] | None = None,
) -> Mapping[str, Any]:
    """Return a serialisable timeline entry for UI rendering."""

    payload: Dict[str, Any] = {
        "phase": phase,
        "title": title,
        "description": description,
        "status": status,
    }
    if time_label:
        payload["time"] = time_label
    if metrics:
        payload["metrics"] = dict(metrics)
    return payload


def _format_time_label(raw: Optional[str]) -> Optional[str]:
    if not raw:
        return None
    try:
        stamp = datetime.fromisoformat(raw.replace("Z", "+00:00"))
    except ValueError:
        return None
    return stamp.strftime("%H:%M:%S")


def _normalise_batch_status(status: Optional[str]) -> str:
    value = (status or "info").lower()
    if value in {"ok", "success"}:
        return "success"
    if value in {"warning", "warn"}:
        return "warning"
    if value in {"error", "block", "danger"}:
        return "danger"
    if value in {"idle", "skipped"}:
        return "info"
    return value or "info"


def _timeline_from_batches(
    batches: Iterable[Mapping[str, Any]],
    *,
    phase: str,
) -> List[Mapping[str, Any]]:
    events: List[Mapping[str, Any]] = []
    for batch in batches:
        if not isinstance(batch, Mapping):
            continue
        batch_id = batch.get("batch_id")
        row_count = int(batch.get("row_count", 0) or 0)
        status = _normalise_batch_status(batch.get("status"))
        violations = int(batch.get("violations", 0) or 0)
        metrics = {"rows": row_count, "violations": violations}
        description = "Micro-batch processed"
        if status == "info" and row_count == 0:
            description = "Empty micro-batch heartbeat"
        elif status == "warning":
            description = "Validation produced warnings"
        elif status == "danger":
            description = "Validation blocked this batch"
        info_lines: List[str] = []
        errors = batch.get("errors")
        warnings = batch.get("warnings")
        if isinstance(errors, list) and errors:
            info_lines.append("; ".join(str(err) for err in errors))
        if isinstance(warnings, list) and warnings:
            info_lines.append("; ".join(str(item) for item in warnings))
        intervention = batch.get("intervention")
        if isinstance(intervention, str) and intervention:
            info_lines.append(f"Intervention: {intervention}")
        if info_lines:
            description = f"{description}. {' '.join(info_lines)}"
        title = f"Batch #{batch_id}" if batch_id is not None else "Streaming batch"
        time_label = None
        timestamp = batch.get("timestamp")
        if isinstance(timestamp, str):
            time_label = _format_time_label(timestamp)
        events.append(
            _timeline_event(
                phase=phase,
                title=title,
                description=description,
                time_label=time_label,
                status=status,
                metrics=metrics,
            )
        )
    return events


def _streaming_row_count(
    metrics: Mapping[str, Any] | None,
    batches: Iterable[Mapping[str, Any]],
) -> int:
    """Return a best-effort row count for streaming validations."""

    total = 0
    if isinstance(metrics, Mapping):
        candidate = metrics.get("row_count")
        if isinstance(candidate, (int, float)):
            total = int(candidate)
    if total > 0:
        return total
    fallback = 0
    for batch in batches:
        if not isinstance(batch, Mapping):
            continue
        value = batch.get("row_count", 0)
        if isinstance(value, (int, float)):
            fallback += int(value)
    return fallback


def _prepare_demo_streaming_batches(
    batches: Iterable[Mapping[str, Any]],
) -> List[Mapping[str, Any]]:
    """Normalise streaming batches and inject synthetic warnings for the demo."""

    normalised: List[Mapping[str, Any]] = []
    for item in batches:
        if not isinstance(item, Mapping):
            continue
        entry = dict(item)
        entry["status"] = _normalise_batch_status(entry.get("status"))
        normalised.append(entry)

    success_indices = [
        index for index, entry in enumerate(normalised) if entry.get("status") == "success"
    ]
    if not success_indices:
        for index, entry in enumerate(normalised):
            if entry.get("status") in {"info", "success"}:
                updated = dict(entry)
                updated["status"] = "success"
                normalised[index] = updated
                success_indices.append(index)
                break
        else:
            if normalised:
                success_entry = dict(normalised[0])
                success_entry["status"] = "success"
                normalised.insert(0, success_entry)
                success_indices = [0]

    has_warning = any(entry.get("status") == "warning" for entry in normalised)

    def _clone_with_warning(entry: Mapping[str, Any]) -> Mapping[str, Any]:
        mutated = dict(entry)
        mutated["status"] = "warning"
        row_count = int(mutated.get("row_count", 0) or 0)
        violations = int(mutated.get("violations", 0) or 0)
        if violations <= 0:
            violations = max(1, row_count // 6 or 1)
            mutated["violations"] = violations
        metrics = mutated.get("metrics")
        if isinstance(metrics, Mapping):
            metrics_map = dict(metrics)
        else:
            metrics_map = {}
        metrics_map.setdefault("violations.synthetic_demo", mutated.get("violations", 0))
        mutated["metrics"] = metrics_map
        warnings = mutated.get("warnings")
        if isinstance(warnings, list):
            warning_list = list(warnings)
        elif warnings:
            warning_list = [str(warnings)]
        else:
            warning_list = []
        warning_list.append(
            "Synthetic demo warning: streaming batch included simulated expectation alerts."
        )
        mutated["warnings"] = warning_list
        return mutated

    if not has_warning and normalised:
        candidate_index: int | None = None
        if success_indices:
            for index in success_indices:
                row_count = int(normalised[index].get("row_count", 0) or 0)
                if row_count > 0:
                    candidate_index = index
                    break
            if candidate_index is None:
                candidate_index = success_indices[0]
        else:
            for index, entry in enumerate(normalised):
                if entry.get("status") == "info":
                    candidate_index = index
                    break
            if candidate_index is None:
                candidate_index = len(normalised) - 1

        base_entry = normalised[candidate_index]
        warning_entry = _clone_with_warning(base_entry)
        normalised.insert(candidate_index + 1, warning_entry)

    return normalised


def _batch_dataset_version(
    base_version: str,
    batch: Mapping[str, Any],
    index: int,
) -> str:
    """Return a dataset version identifier for a micro-batch."""

    timestamp = batch.get("timestamp")
    if isinstance(timestamp, str) and timestamp:
        return timestamp
    if base_version:
        return f"{base_version}#batch-{index}"
    return f"batch-{index}"


def _batch_status_to_dataset_status(status: str) -> str:
    mapping = {"success": "ok", "info": "ok", "warning": "warning", "danger": "error"}
    return mapping.get(status, "ok")


def _downgrade_streaming_batches(
    batches: Iterable[Mapping[str, Any]] | None,
) -> List[Mapping[str, Any]]:
    """Return a serialisable copy of ``batches`` with friendly status labels."""

    normalised: List[Mapping[str, Any]] = []
    if not batches:
        return normalised
    for item in batches:
        if not isinstance(item, Mapping):
            continue
        entry = dict(item)
        status = _normalise_batch_status(entry.get("status"))
        if status == "danger":
            status = "warning"
        entry["status"] = status
        normalised.append(entry)
    return normalised


def _record_result(
    result: _ScenarioResult,
    *,
    scenario_key: str,
    run_type: str,
    contract_id: str,
    contract_version: str,
) -> tuple[str, str]:
    dataset_name = result.dataset_name
    dataset_version = result.dataset_version or ""
    validation = result.validation
    status_value = _normalise_status(validation)
    dq_details = dict(result.dq_details)
    if result.timeline:
        dq_details["timeline"] = list(result.timeline)
    violations = _extract_violation_count(dq_details.get("output"))
    output_details = dq_details.get("output") if isinstance(dq_details, Mapping) else {}
    streaming_batches: List[Mapping[str, Any]] = []
    if isinstance(output_details, Mapping):
        batches_payload = output_details.get("streaming_batches")
        if isinstance(batches_payload, list):
            streaming_batches = [
                item for item in batches_payload if isinstance(item, Mapping)
            ]

    seen_versions = {dataset_version} if dataset_version else set()
    extra_records: List[DatasetRecord] = []
    records_to_register: List[DatasetRecord] = []
    for index, batch in enumerate(streaming_batches):
        row_count = int(batch.get("row_count", 0) or 0)
        errors = batch.get("errors")
        warnings = batch.get("warnings")
        if row_count <= 0 and not errors and not warnings:
            continue
        batch_status = _batch_status_to_dataset_status(
            _normalise_batch_status(batch.get("status"))
        )
        batch_version = _batch_dataset_version(dataset_version, batch, index)
        if batch_version in seen_versions:
            continue
        seen_versions.add(batch_version)
        batch_details = {
            "output": {
                "streaming_batch": dict(batch),
                "streaming_batch_id": batch.get("batch_id"),
            }
        }
        extra_records.append(
            DatasetRecord(
                contract_id=contract_id,
                contract_version=contract_version,
                dataset_name=dataset_name,
                dataset_version=batch_version,
                status=batch_status,
                dq_details=batch_details,
                run_type=f"{run_type}-batch" if run_type else "batch",
                violations=int(batch.get("violations", 0) or 0),
                scenario_key=scenario_key,
            )
        )

    record = DatasetRecord(
        contract_id=contract_id,
        contract_version=contract_version,
        dataset_name=dataset_name,
        dataset_version=dataset_version,
        status=status_value,
        dq_details=dq_details,
        run_type=run_type,
        violations=violations,
        scenario_key=scenario_key,
    )
    if result.status_reason:
        record.reason = result.status_reason

    records_to_register.extend(extra_records)

    for side in result.related or []:
        side_version = side.dataset_version or ""
        side_record = DatasetRecord(
            contract_id=side.contract_id,
            contract_version=side.contract_version,
            dataset_name=side.dataset_name,
            dataset_version=side_version,
            status=side.status,
            dq_details=dict(side.dq_details),
            run_type=side.run_type,
            violations=side.violations,
            scenario_key=scenario_key,
        )
        if side.reason:
            side_record.reason = side.reason
        records_to_register.append(side_record)
        _ensure_streaming_version(side.dataset_name, side.dataset_version)

    records_to_register.append(record)
    _ensure_streaming_version(dataset_name, result.dataset_version)
    return dataset_name, dataset_version


def _scenario_valid(
    seconds: int,
    *,
    run_type: str,
    progress: Optional[StreamingProgressReporter] = None,
) -> _ScenarioResult:
    spark = _spark_session()
    started_at = datetime.now(timezone.utc)
    dataset_version = started_at.isoformat()
    scenario_key = "streaming-valid"

    def _emit(event: Mapping[str, Any]) -> None:
        payload = dict(event)
        payload.setdefault("scenario", scenario_key)
        _progress_emit(progress, payload)

    _emit(
        {
            "type": "stage",
            "phase": "Setup",
            "title": "Preparing healthy streaming pipeline",
            "description": f"Processing micro-batches for approximately {seconds} seconds.",
            "status": "info",
            "metrics": {"seconds": seconds},
        }
    )

    read_request = GovernanceSparkReadRequest(
        context=GovernanceReadContext(
            contract={
                "contract_id": _INPUT_CONTRACT,
                "version_selector": f"=={_CONTRACT_VERSIONS[_INPUT_CONTRACT]}",
            }
        ),
        dataset_locator=StaticDatasetLocator(dataset_version=None),
        options={"rowsPerSecond": "6", "numPartitions": "1"},
    )
    df, read_status = read_stream_with_governance(
        spark,
        read_request,
        governance_service=governance_service,
    )
    input_details = _sanitize_validation_details(
        read_status.details if read_status else {}
    )
    input_details.setdefault("dataset_id", _INPUT_CONTRACT)
    input_version = input_details.get("dataset_version")
    if not isinstance(input_version, str) or not input_version:
        input_version = dataset_version
        input_details["dataset_version"] = input_version
    _ensure_streaming_version(_INPUT_CONTRACT, input_version)
    _emit(
        {
            "type": "stage",
            "phase": "Read",
            "title": "Streaming read attached",
            "description": "Synthetic rate source aligned with input contract.",
            "status": "success",
        }
    )

    processed_df = df.withColumn("quality_flag", F.lit("valid"))
    checkpoint = _checkpoint_dir("valid", version=dataset_version)

    def _forward(event: Mapping[str, Any]) -> None:
        _emit(event)

    write_request = GovernanceSparkWriteRequest(
        context=GovernanceWriteContext(
            contract={
                "contract_id": _OUTPUT_CONTRACT,
                "version_selector": f"=={_CONTRACT_VERSIONS[_OUTPUT_CONTRACT]}",
            }
        ),
        dataset_locator=StaticDatasetLocator(dataset_version=dataset_version),
        options={
            "checkpointLocation": str(checkpoint),
            "queryName": f"demo_stream_valid_{dataset_version}",
        },
    )
    validation = write_stream_with_governance(
        df=processed_df,
        request=write_request,
        governance_service=governance_service,
        enforce=run_type == "enforce",
        on_streaming_batch=_forward,
    )
    queries = _extract_query_handles(validation.details)
    if queries:
        _emit(
            {
                "type": "stage",
                "phase": "Validation",
                "title": "Validation running",
                "description": "Primary sink and observation writer streaming queries started.",
                "status": "info",
            }
        )
    try:
        _emit(
            {
                "type": "stage",
                "phase": "Streaming",
                "title": "Advancing micro-batches",
                "description": "Driving streaming queries to accumulate contract metrics.",
                "status": "info",
            }
        )
        _drive_queries(queries, seconds=seconds)
    finally:
        _stop_queries(queries)
    _emit(
        {
            "type": "stage",
            "phase": "Streaming",
            "title": "Streaming window complete",
            "description": "Streaming queries drained for the configured interval.",
            "status": "success",
        }
    )

    details = _serialise_streaming_details(validation.details, queries=queries)
    batches: List[Mapping[str, Any]] = []
    candidate_batches = details.get("streaming_batches")
    if isinstance(candidate_batches, list):
        batches = _prepare_demo_streaming_batches(candidate_batches)
        if batches:
            details["streaming_batches"] = [dict(item) for item in batches]
    dq_details: MutableMapping[str, Any] = {
        "input": input_details,
        "output": details,
    }
    metrics = dict(validation.metrics or {})
    violations_total = sum(
        int(value)
        for key, value in metrics.items()
        if key.startswith("violations.") and isinstance(value, (int, float))
    )
    if batches:
        warning_total = sum(
            int(entry.get("violations", 0) or 0)
            for entry in batches
            if entry.get("status") == "warning"
        )
        violations_total = max(violations_total, warning_total)
    total_rows = _streaming_row_count(metrics, batches)
    timeline = [
        _timeline_event(
            phase="Source",
            title="Synthetic event rate stream started",
            description="Spark's rate source emits timestamp/value rows at 6 events per second across a single partition.",
            time_label=started_at.strftime("%H:%M:%S"),
            status="info",
            metrics={"rows_per_second": 6, "partitions": 1},
        ),
        _timeline_event(
            phase="Processing",
            title="Events enriched before validation",
            description="Incoming rows gain a quality_flag column prior to contract checks.",
            status="success",
        ),
        *_timeline_from_batches(batches, phase="Micro-batch"),
        _timeline_event(
            phase="Validation",
            title="Contract checks applied to running stream",
            description="Each micro-batch is validated against demo.streaming.events_processed while streaming metrics are captured.",
            status="success" if validation.ok else "warning",
            metrics={
                "row_count": total_rows,
                "failed_expectations": violations_total,
                "last_batch_id": details.get("streaming_batch_id"),
            },
        ),
        _timeline_event(
            phase="Governance",
            title="Dataset version registered",
            description="Governance records the validated slice and exposes its metrics to downstream consumers.",
            status="success" if validation.ok else "warning",
            time_label=_format_time_label(details.get("dataset_version")),
            metrics={
                "dataset_id": _OUTPUT_CONTRACT,
                "dataset_version": details.get("dataset_version") or dataset_version,
            },
        ),
    ]
    _emit(
        {
            "type": "stage",
            "phase": "Governance",
            "title": "Dataset version registered",
            "description": "Governance catalog updated with streaming validation results.",
            "status": "success" if validation.ok else "warning",
            "metrics": {
                "dataset_version": details.get("dataset_version") or dataset_version,
                "batches": len(batches),
            },
        }
    )
    _emit(
        {
            "type": "complete",
            "status": "ok" if validation.ok else "warning",
            "dataset_name": _OUTPUT_CONTRACT,
            "dataset_version": details.get("dataset_version") or dataset_version,
        }
    )

    input_record = _RelatedDataset(
        contract_id=_INPUT_CONTRACT,
        contract_version=_CONTRACT_VERSIONS[_INPUT_CONTRACT],
        dataset_name=_INPUT_CONTRACT,
        dataset_version=input_version,
        status=_normalise_status(read_status),
        dq_details={
            "stream": "input",
            "details": input_details,
            "metrics": dict(read_status.metrics or {}),
        },
        run_type=f"{run_type}-input" if run_type else "input",
        violations=_extract_violation_count(input_details),
    )

    return _ScenarioResult(
        dataset_name=_OUTPUT_CONTRACT,
        dataset_version=details.get("dataset_version") or dataset_version,
        validation=validation,
        queries=queries,
        dq_details=dq_details,
        timeline=timeline,
        related=[input_record],
    )


def _scenario_dq_rejects(
    seconds: int,
    *,
    run_type: str,
    progress: Optional[StreamingProgressReporter] = None,
) -> _ScenarioResult:
    spark = _spark_session()
    started_at = datetime.now(timezone.utc)
    dataset_version = started_at.isoformat()
    scenario_key = "streaming-dq-rejects"

    def _emit(event: Mapping[str, Any]) -> None:
        payload = dict(event)
        payload.setdefault("scenario", scenario_key)
        _progress_emit(progress, payload)

    _emit(
        {
            "type": "stage",
            "phase": "Setup",
            "title": "Routing rejects without blocking",
            "description": f"Flip every fourth event negative while streaming for {seconds} seconds.",
            "status": "info",
            "metrics": {"seconds": seconds},
        }
    )

    read_request = GovernanceSparkReadRequest(
        context=GovernanceReadContext(
            contract={
                "contract_id": _INPUT_CONTRACT,
                "version_selector": f"=={_CONTRACT_VERSIONS[_INPUT_CONTRACT]}",
            },
        ),
        dataset_locator=StaticDatasetLocator(dataset_version=None),
        options={"rowsPerSecond": "6", "numPartitions": "1"},
    )
    df, read_status = read_stream_with_governance(
        spark,
        read_request,
        governance_service=governance_service,
    )
    input_details = _sanitize_validation_details(
        read_status.details if read_status else {}
    )
    input_details.setdefault("dataset_id", _INPUT_CONTRACT)
    input_version = input_details.get("dataset_version")
    if not isinstance(input_version, str) or not input_version:
        input_version = dataset_version
        input_details["dataset_version"] = input_version
    _ensure_streaming_version(_INPUT_CONTRACT, input_version)
    _emit(
        {
            "type": "stage",
            "phase": "Read",
            "title": "Streaming read attached",
            "description": "Rate source aligned with input contract before mutation.",
            "status": "success",
        }
    )

    mutated_df = (
        df.withColumn("quality_cycle", F.floor(F.col("value") / F.lit(12)) % 2)
        .withColumn(
            "value",
            F.when(F.col("quality_cycle") == 1, -F.col("value")).otherwise(F.col("value")),
        )
        .withColumn(
            "quality_flag",
            F.when(F.col("quality_cycle") == 1, F.lit("warning")).otherwise(F.lit("valid")),
        )
        .drop("quality_cycle")
    )
    checkpoint = _checkpoint_dir("dq", version=dataset_version)

    def _forward(event: Mapping[str, Any]) -> None:
        _emit(event)

    write_request = GovernanceSparkWriteRequest(
        context=GovernanceWriteContext(
            contract={
                "contract_id": _OUTPUT_CONTRACT,
                "version_selector": f"=={_CONTRACT_VERSIONS[_OUTPUT_CONTRACT]}",
            },
            dataset_id=_OUTPUT_CONTRACT,
            dataset_version=dataset_version,
        ),
        dataset_locator=StaticDatasetLocator(dataset_version=dataset_version),
        options={
            "checkpointLocation": str(checkpoint),
            "queryName": f"demo_stream_dq_{dataset_version}",
        },
    )
    validation = write_stream_with_governance(
        df=mutated_df,
        request=write_request,
        governance_service=governance_service,
        enforce=run_type == "enforce",
        on_streaming_batch=_forward,
    )
    _emit(
        {
            "type": "stage",
            "phase": "Validation",
            "title": "Processed stream carrying warnings",
            "description": "Primary sink continues even though validation flags negative values.",
            "status": "warning",
        }
    )

    reject_checkpoint = _checkpoint_dir("dq_rejects", version=dataset_version)
    reject_df = mutated_df.filter(F.col("value") < 0).select(
        "timestamp",
        "value",
        F.lit("value below zero").alias("reject_reason"),
    )
    _, reject_target = _dataset_version_paths(_REJECT_DATASET, dataset_version)
    reject_target.mkdir(parents=True, exist_ok=True)
    _write_version_marker(reject_target, dataset_version)
    reject_batches: List[Dict[str, Any]] = []
    reject_total_rows = 0

    def _write_reject_batch(batch_df, batch_id: int) -> None:
        nonlocal reject_total_rows
        materialised = batch_df.persist()
        try:
            try:
                row_count = materialised.count()
            except Exception:  # pragma: no cover - Spark can interrupt active jobs
                logger.exception("Failed to count reject batch %s", batch_id)
                row_count = 0
            if row_count:
                try:
                    materialised.write.mode("append").parquet(str(reject_target))
                except Exception:  # pragma: no cover - best-effort reject persistence
                    logger.exception("Failed to persist reject batch %s", batch_id)
            reject_total_rows += row_count
        finally:
            materialised.unpersist()

        status = "warning" if row_count else "info"
        batch_event = {
            "batch_id": batch_id,
            "row_count": row_count,
            "violations": row_count,
            "status": status,
        }
        reject_batches.append(batch_event)
        _emit(
            {
                "type": "batch",
                "phase": "Rejects",
                "status": status,
                "stream": "reject",
                "metrics": {
                    "batch_id": batch_id,
                    "row_count": row_count,
                    "dataset_version": dataset_version,
                },
            }
        )

    reject_query = (
        reject_df.writeStream.foreachBatch(_write_reject_batch)
        .outputMode("append")
        .queryName(f"demo_stream_dq_rejects_{dataset_version}")
        .option("checkpointLocation", str(reject_checkpoint))
        .start()
    )
    _emit(
        {
            "type": "stage",
            "phase": "Rejects",
            "title": "Reject sink streaming",
            "description": "Invalid rows copied into the ungoverned demo.streaming.events_rejects folder with reasons.",
            "status": "info",
        }
    )

    main_queries = _extract_query_handles(validation.details)
    queries: List[StreamingQuery] = [*main_queries, reject_query]
    try:
        _emit(
            {
                "type": "stage",
                "phase": "Streaming",
                "title": "Advancing micro-batches",
                "description": "Primary sink continues while rejects accumulate.",
                "status": "info",
            }
        )
        _drive_queries(queries, seconds=seconds)
    finally:
        _stop_queries(queries)
    _emit(
        {
            "type": "stage",
            "phase": "Streaming",
            "title": "Streaming window complete",
            "description": "Micro-batches drained; review processed and reject counts.",
            "status": "success",
        }
    )

    details = _serialise_streaming_details(validation.details, queries=main_queries)
    streaming_batches = _downgrade_streaming_batches(details.get("streaming_batches"))
    if streaming_batches:
        details["streaming_batches"] = streaming_batches
    validation.status = "warning"
    validation.details = details
    reject_details: Dict[str, Any] = {
        "dataset_id": _REJECT_DATASET,
        "dataset_version": dataset_version,
        "row_count": reject_total_rows,
        "streaming_batches": reject_batches,
        "path": str(reject_target),
        "governed": False,
    }
    batches: List[Mapping[str, Any]] = []
    batches = streaming_batches
    reject_batches_serialised: List[Mapping[str, Any]] = [
        item for item in reject_batches if isinstance(item, Mapping)
    ]
    reject_count = reject_total_rows
    dq_details: MutableMapping[str, Any] = {
        "input": input_details,
        "output": details,
        "rejects": {
            **reject_details,
            "streaming_batches": reject_batches_serialised,
            "row_count": reject_count,
        },
    }
    metrics = dict(validation.metrics or {})
    violations_total = sum(
        int(value)
        for key, value in metrics.items()
        if key.startswith("violations.") and isinstance(value, (int, float))
    )
    if batches:
        warning_total = sum(
            int(entry.get("violations", 0) or 0)
            for entry in batches
            if entry.get("status") == "warning"
        )
        violations_total = max(violations_total, warning_total)
    total_rows = _streaming_row_count(metrics, batches)
    timeline = [
        _timeline_event(
            phase="Source",
            title="Synthetic event rate stream started",
            description="6 events per second land in demo.streaming.events before validation.",
            time_label=started_at.strftime("%H:%M:%S"),
            status="info",
            metrics={"rows_per_second": 6, "partitions": 1},
        ),
        _timeline_event(
            phase="Processing",
            title="Quality flags applied to streaming rows",
            description="Alternating micro-batch cycles flip values negative so rejects accumulate without stopping the stream.",
            status="warning",
        ),
        *_timeline_from_batches(batches, phase="Micro-batch"),
        _timeline_event(
            phase="Validation",
            title="Contract checks flag warning rows",
            description="Negative values fail ge_value but enforcement is relaxed so the stream continues.",
            status="warning" if violations_total else "success",
            metrics={
                "row_count": total_rows,
                "failed_expectations": violations_total,
                "last_batch_id": details.get("streaming_batch_id"),
            },
        ),
        *_timeline_from_batches(reject_batches_serialised, phase="Rejects"),
        _timeline_event(
            phase="Rejects",
            title="Rejected rows copied to demo.streaming.events_rejects",
            description="The demo reject sink captures failing rows with their reason column.",
            status="warning" if reject_count else "info",
            metrics={"reject_rows": reject_count, "path": str(reject_target)},
        ),
        _timeline_event(
            phase="Governance",
            title="Processed slice recorded with warning",
            description="Governance keeps the processed dataset in a warning state while rejects accumulate for remediation.",
            status="warning",
            time_label=_format_time_label(details.get("dataset_version")),
            metrics={
                "dataset_id": _OUTPUT_CONTRACT,
                "dataset_version": details.get("dataset_version") or dataset_version,
            },
        ),
    ]
    _emit(
        {
            "type": "stage",
            "phase": "Governance",
            "title": "Dataset version registered",
            "description": "Governance captures warning status and reject metadata.",
            "status": "warning",
            "metrics": {
                "dataset_version": details.get("dataset_version") or dataset_version,
                "reject_rows": reject_count,
            },
        }
    )
    _emit(
        {
            "type": "complete",
            "status": "warning",
            "dataset_name": _OUTPUT_CONTRACT,
            "dataset_version": details.get("dataset_version") or dataset_version,
        }
    )

    input_record = _RelatedDataset(
        contract_id=_INPUT_CONTRACT,
        contract_version=_CONTRACT_VERSIONS[_INPUT_CONTRACT],
        dataset_name=_INPUT_CONTRACT,
        dataset_version=input_version,
        status=_normalise_status(read_status),
        dq_details={
            "stream": "input",
            "details": input_details,
            "metrics": dict(read_status.metrics or {}),
        },
        run_type=f"{run_type}-input" if run_type else "input",
        violations=_extract_violation_count(input_details),
    )
    return _ScenarioResult(
        dataset_name=_OUTPUT_CONTRACT,
        dataset_version=details.get("dataset_version") or dataset_version,
        validation=validation,
        queries=queries,
        dq_details=dq_details,
        timeline=timeline,
        related=[input_record],
    )


def _scenario_schema_break(
    seconds: int,
    *,
    run_type: str,
    progress: Optional[StreamingProgressReporter] = None,
) -> _ScenarioResult:
    spark = _spark_session()
    started_at = datetime.now(timezone.utc)
    dataset_version = started_at.isoformat()
    scenario_key = "streaming-schema-break"

    def _emit(event: Mapping[str, Any]) -> None:
        payload = dict(event)
        payload.setdefault("scenario", scenario_key)
        _progress_emit(progress, payload)

    _emit(
        {
            "type": "stage",
            "phase": "Setup",
            "title": "Schema break blocks the stream",
            "description": "Drop a required column and observe enforcement halt the pipeline.",
            "status": "info",
        }
    )

    read_request = GovernanceSparkReadRequest(
        context=GovernanceReadContext(
            contract={
                "contract_id": _INPUT_CONTRACT,
                "version_selector": f"=={_CONTRACT_VERSIONS[_INPUT_CONTRACT]}",
            },
        ),
        dataset_locator=StaticDatasetLocator(dataset_version=None),
        options={"rowsPerSecond": "6", "numPartitions": "1"},
    )
    df, read_status = read_stream_with_governance(
        spark,
        read_request,
        governance_service=governance_service,
    )
    input_details = _sanitize_validation_details(
        read_status.details if read_status else {}
    )
    input_details.setdefault("dataset_id", _INPUT_CONTRACT)
    input_version = input_details.get("dataset_version")
    if not isinstance(input_version, str) or not input_version:
        input_version = dataset_version
        input_details["dataset_version"] = input_version
    _ensure_streaming_version(_INPUT_CONTRACT, input_version)
    _emit(
        {
            "type": "stage",
            "phase": "Read",
            "title": "Streaming read attached",
            "description": "Rate source aligned before the schema break is introduced.",
            "status": "success",
        }
    )

    input_record = _RelatedDataset(
        contract_id=_INPUT_CONTRACT,
        contract_version=_CONTRACT_VERSIONS[_INPUT_CONTRACT],
        dataset_name=_INPUT_CONTRACT,
        dataset_version=input_version,
        status=_normalise_status(read_status),
        dq_details={
            "stream": "input",
            "details": input_details,
            "metrics": dict(read_status.metrics or {}),
        },
        run_type=f"{run_type}-input" if run_type else "input",
        violations=_extract_violation_count(input_details),
    )

    broken_df = df.drop("value")
    checkpoint = _checkpoint_dir("schema_break", version=dataset_version)
    try:
        write_request = GovernanceSparkWriteRequest(
            context=GovernanceWriteContext(
                contract={
                    "contract_id": _OUTPUT_CONTRACT,
                    "version_selector": f"=={_CONTRACT_VERSIONS[_OUTPUT_CONTRACT]}",
                },
                dataset_id=_OUTPUT_CONTRACT,
                dataset_version=dataset_version,
            ),
            dataset_locator=StaticDatasetLocator(dataset_version=dataset_version),
            options={
                "checkpointLocation": str(checkpoint),
                "queryName": f"demo_stream_schema_{dataset_version}",
            },
        )
        validation = write_stream_with_governance(
            df=broken_df,
            request=write_request,
            governance_service=governance_service,
            enforce=run_type == "enforce",
        )
        queries = _extract_query_handles(validation.details)
        try:
            _drive_queries(queries, seconds=seconds)
        finally:
            _stop_queries(queries)
        details = _serialise_streaming_details(validation.details, queries=queries)
        batches: List[Mapping[str, Any]] = []
        candidate_batches = details.get("streaming_batches")
        if isinstance(candidate_batches, list):
            batches = [item for item in candidate_batches if isinstance(item, Mapping)]
        dq_details: MutableMapping[str, Any] = {
            "input": read_status.details if read_status else {},
            "output": details,
        }
        reason = details.get("errors")
        status_reason = reason[0] if isinstance(reason, list) and reason else None
        timeline = [
            _timeline_event(
                phase="Source",
                title="Synthetic event rate stream started",
                description="Events flow from demo.streaming.events into the processing job at 6 rows per second.",
                time_label=started_at.strftime("%H:%M:%S"),
                status="info",
                metrics={"rows_per_second": 6, "partitions": 1},
            ),
            _timeline_event(
                phase="Processing",
                title="Required column dropped upstream of validation",
                description="A faulty transformation removes the value column before the dataset hits the contract boundary.",
                status="danger",
            ),
            *_timeline_from_batches(batches, phase="Micro-batch"),
            _timeline_event(
                phase="Validation",
                title="Schema mismatch detected",
                description="The outgoing stream dropped the value column so contract alignment fails and the stream halts.",
                status="danger",
                metrics={"missing_columns": ["value"], "last_batch_id": details.get("streaming_batch_id")},
            ),
        ]
        _emit(
            {
                "type": "stage",
                "phase": "Validation",
                "title": "Schema mismatch detected",
                "description": status_reason or "Streaming halted because value column is missing.",
                "status": "error",
            }
        )
        _emit(
            {
                "type": "complete",
                "status": "error",
                "dataset_name": _OUTPUT_CONTRACT,
                "dataset_version": details.get("dataset_version") or dataset_version,
            }
        )
        return _ScenarioResult(
            dataset_name=_OUTPUT_CONTRACT,
            dataset_version=details.get("dataset_version") or dataset_version,
            validation=validation,
            queries=queries,
            dq_details=dq_details,
            timeline=timeline,
            status_reason=status_reason,
            related=[input_record],
        )
    except Exception as exc:
        dq_details = {
            "input": input_details,
            "output": {
                "errors": [str(exc)],
                "status": "block",
            },
        }
        timeline = [
            _timeline_event(
                phase="Source",
                title="Synthetic event rate stream started",
                description="Events attempted to flow into the processing job, but the downstream schema breaks immediately.",
                time_label=started_at.strftime("%H:%M:%S"),
                status="info",
            ),
            _timeline_event(
                phase="Processing",
                title="Processing step dropped required column",
                description="The faulty transformation removes value so validation cannot proceed.",
                status="danger",
            ),
            _timeline_event(
                phase="Validation",
                title="Schema mismatch detected",
                description=str(exc),
                status="danger",
            ),
        ]
        _emit(
            {
                "type": "error",
                "message": str(exc),
            }
        )
        return _ScenarioResult(
            dataset_name=_OUTPUT_CONTRACT,
            dataset_version=None,
            validation=None,
            queries=[],
            dq_details=dq_details,
            timeline=timeline,
            status_reason=str(exc),
            related=[input_record],
        )


_SCENARIO_MAP = {
    "streaming-valid": _scenario_valid,
    "streaming-dq-rejects": _scenario_dq_rejects,
    "streaming-schema-break": _scenario_schema_break,
}


def run_streaming_scenario(
    scenario_key: str,
    *,
    seconds: int = 5,
    run_type: str = "observe",
    progress: Optional[StreamingProgressReporter] = None,
) -> tuple[str, str]:
    """Execute a streaming scenario and record its outcome."""

    runner = _SCENARIO_MAP.get(scenario_key)
    if runner is None:
        raise ValueError(f"Unknown streaming scenario: {scenario_key}")
    result = runner(seconds, run_type=run_type, progress=progress)
    contract_version = _CONTRACT_VERSIONS.get(_OUTPUT_CONTRACT, "")
    return _record_result(
        result,
        scenario_key=scenario_key,
        run_type=run_type,
        contract_id=_OUTPUT_CONTRACT,
        contract_version=contract_version,
    )


__all__ = ["run_streaming_scenario", "StreamingProgressReporter"]
