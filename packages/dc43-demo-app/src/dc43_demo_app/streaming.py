"""Structured Streaming scenarios integrated into the demo application."""
from __future__ import annotations

import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Mapping, MutableMapping, Optional

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.streaming import StreamingQuery, StreamingQueryException

from dc43_integrations.spark.io import (
    StaticDatasetLocator,
    read_stream_with_contract,
    write_stream_with_contract,
)
from dc43_service_clients.data_quality import ValidationResult

from .contracts_api import (
    DatasetRecord,
    contract_service,
    dq_service,
    governance_service,
    load_records,
    refresh_dataset_aliases,
    save_records,
)
from .contracts_workspace import current_workspace


_INPUT_CONTRACT = "demo.streaming.events"
_OUTPUT_CONTRACT = "demo.streaming.events_processed"
_REJECT_CONTRACT = "demo.streaming.events_rejects"
_CONTRACT_VERSIONS: Dict[str, str] = {
    _INPUT_CONTRACT: "0.1.0",
    _OUTPUT_CONTRACT: "0.1.0",
    _REJECT_CONTRACT: "0.1.0",
}


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


def _spark_session() -> SparkSession:
    """Return the shared Spark session for streaming demos."""

    spark = SparkSession.getActiveSession()
    if spark is not None:
        return spark
    spark = (
        SparkSession.builder.master("local[2]")
        .appName("dc43-demo-streaming")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "1")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


def _checkpoint_dir(name: str, *, version: str) -> Path:
    workspace = current_workspace()
    root = workspace.root / "streaming_checkpoints" / name / version
    root.mkdir(parents=True, exist_ok=True)
    return root


def _dataset_version_path(dataset: str, version: str) -> Path:
    """Return the expected filesystem location for ``dataset`` / ``version``."""

    workspace = current_workspace()
    root = workspace.data_dir / dataset
    candidate = root / version
    if candidate.exists():
        return candidate
    safe = "".join(
        ch if ch.isalnum() or ch in {"-", "_", "."} else "_" for ch in version
    )
    return root / safe


def _drive_queries(queries: Iterable[StreamingQuery], *, seconds: int) -> None:
    """Advance ``queries`` for roughly ``seconds`` seconds."""

    active_queries = list(queries)
    if not active_queries:
        return
    # Ensure at least one micro-batch is processed even for short runs so that
    # metrics and validation details have a chance to update.
    for query in active_queries:
        try:
            query.processAllAvailable()
        except StreamingQueryException:
            raise
    deadline = time.time() + max(seconds, 0)
    while time.time() < deadline:
        for query in active_queries:
            try:
                query.processAllAvailable()
            except StreamingQueryException:
                # Propagate failures to the caller after stopping all queries so the
                # dataset record can capture the reason from the validation payload.
                raise
        time.sleep(0.2)


def _stop_queries(queries: Iterable[StreamingQuery]) -> None:
    for query in queries:
        try:
            if query.isActive:
                query.stop()
        except Exception:
            continue


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


def _normalise_status(validation: Optional[ValidationResult]) -> str:
    if validation is None:
        return "error"
    status = (validation.status or "").lower()
    if status in {"warn", "warning"}:
        return "warning"
    if status in {"block", "error"}:
        return "error"
    if validation.errors:
        return "error"
    if validation.warnings:
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
    records = load_records()
    dq_details = dict(result.dq_details)
    if result.timeline:
        dq_details["timeline"] = list(result.timeline)
    violations = _extract_violation_count(dq_details.get("output"))
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
    records.append(record)
    save_records(records)
    if dataset_name and dataset_version:
        refresh_dataset_aliases(dataset_name)
    return dataset_name, dataset_version


def _scenario_valid(seconds: int, *, run_type: str) -> _ScenarioResult:
    spark = _spark_session()
    started_at = datetime.now(timezone.utc)
    dataset_version = started_at.isoformat()
    df, read_status = read_stream_with_contract(
        spark=spark,
        contract_id=_INPUT_CONTRACT,
        contract_service=contract_service,
        expected_contract_version=f"=={_CONTRACT_VERSIONS[_INPUT_CONTRACT]}",
        data_quality_service=dq_service,
        governance_service=governance_service,
        dataset_locator=StaticDatasetLocator(dataset_version=None),
        options={"rowsPerSecond": "6", "numPartitions": "1"},
    )
    processed_df = df.withColumn("quality_flag", F.lit("valid"))
    checkpoint = _checkpoint_dir("valid", version=dataset_version)
    validation = write_stream_with_contract(
        df=processed_df,
        contract_id=_OUTPUT_CONTRACT,
        contract_service=contract_service,
        expected_contract_version=f"=={_CONTRACT_VERSIONS[_OUTPUT_CONTRACT]}",
        data_quality_service=dq_service,
        governance_service=governance_service,
        dataset_locator=StaticDatasetLocator(dataset_version=dataset_version),
        options={
            "checkpointLocation": str(checkpoint),
            "queryName": f"demo_stream_valid_{dataset_version}",
        },
    )
    queries = list(validation.details.get("streaming_queries", []) or [])
    try:
        _drive_queries(queries, seconds=seconds)
    finally:
        _stop_queries(queries)
    details = dict(validation.details)
    details["streaming_queries"] = _query_metadata(queries)
    dq_details: MutableMapping[str, Any] = {
        "input": read_status.details if read_status else {},
        "output": details,
    }
    metrics = dict(validation.metrics or {})
    violations_total = sum(
        int(value)
        for key, value in metrics.items()
        if key.startswith("violations.") and isinstance(value, (int, float))
    )
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
            phase="Validation",
            title="Contract checks applied to running stream",
            description="Each micro-batch is validated against demo.streaming.events_processed while streaming metrics are captured.",
            status="success" if validation.ok else "warning",
            metrics={
                "row_count": metrics.get("row_count", 0),
                "failed_expectations": violations_total,
                "last_batch_id": details.get("streaming_batch_id"),
            },
        ),
    ]
    return _ScenarioResult(
        dataset_name=_OUTPUT_CONTRACT,
        dataset_version=details.get("dataset_version") or dataset_version,
        validation=validation,
        queries=queries,
        dq_details=dq_details,
        timeline=timeline,
    )


def _scenario_dq_rejects(seconds: int, *, run_type: str) -> _ScenarioResult:
    spark = _spark_session()
    started_at = datetime.now(timezone.utc)
    dataset_version = started_at.isoformat()
    df, read_status = read_stream_with_contract(
        spark=spark,
        contract_id=_INPUT_CONTRACT,
        contract_service=contract_service,
        expected_contract_version=f"=={_CONTRACT_VERSIONS[_INPUT_CONTRACT]}",
        data_quality_service=dq_service,
        governance_service=governance_service,
        dataset_locator=StaticDatasetLocator(dataset_version=None),
        options={"rowsPerSecond": "6", "numPartitions": "1"},
    )
    mutated_df = df.withColumn(
        "value",
        F.when(F.col("value") % 4 == 0, -F.col("value")).otherwise(F.col("value")),
    ).withColumn(
        "quality_flag",
        F.when(F.col("value") < 0, F.lit("warning")).otherwise(F.lit("valid")),
    )
    checkpoint = _checkpoint_dir("dq", version=dataset_version)
    validation = write_stream_with_contract(
        df=mutated_df,
        contract_id=_OUTPUT_CONTRACT,
        contract_service=contract_service,
        expected_contract_version=f"=={_CONTRACT_VERSIONS[_OUTPUT_CONTRACT]}",
        data_quality_service=dq_service,
        governance_service=governance_service,
        dataset_locator=StaticDatasetLocator(dataset_version=dataset_version),
        options={
            "checkpointLocation": str(checkpoint),
            "queryName": f"demo_stream_dq_{dataset_version}",
        },
        enforce=False,
    )
    reject_checkpoint = _checkpoint_dir("dq_rejects", version=dataset_version)
    reject_df = mutated_df.filter(F.col("value") < 0).select(
        "timestamp",
        "value",
        F.lit("value below zero").alias("reject_reason"),
    )
    reject_result = write_stream_with_contract(
        df=reject_df,
        contract_id=_REJECT_CONTRACT,
        contract_service=contract_service,
        expected_contract_version=f"=={_CONTRACT_VERSIONS[_REJECT_CONTRACT]}",
        data_quality_service=dq_service,
        governance_service=governance_service,
        dataset_locator=StaticDatasetLocator(dataset_version=dataset_version),
        options={
            "checkpointLocation": str(reject_checkpoint),
            "queryName": f"demo_stream_dq_rejects_{dataset_version}",
        },
    )
    queries: List[StreamingQuery] = []
    queries.extend(validation.details.get("streaming_queries", []) or [])
    queries.extend(reject_result.details.get("streaming_queries", []) or [])
    try:
        _drive_queries(queries, seconds=seconds)
    finally:
        _stop_queries(queries)
    details = dict(validation.details)
    reject_details = dict(reject_result.details)
    details["streaming_queries"] = _query_metadata(queries)
    reject_path = _dataset_version_path(_REJECT_CONTRACT, dataset_version)
    reject_count = 0
    if reject_path.exists():
        try:
            reject_count = spark.read.format("parquet").load(str(reject_path)).count()
        except Exception:
            reject_count = 0
    dq_details: MutableMapping[str, Any] = {
        "input": read_status.details if read_status else {},
        "output": details,
        "rejects": {
            **reject_details,
            "row_count": reject_count,
        },
    }
    metrics = dict(validation.metrics or {})
    violations_total = sum(
        int(value)
        for key, value in metrics.items()
        if key.startswith("violations.") and isinstance(value, (int, float))
    )
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
            phase="Validation",
            title="Contract checks flag warning rows",
            description="Negative values fail ge_value but enforcement is relaxed so the stream continues.",
            status="warning" if violations_total else "success",
            metrics={
                "row_count": metrics.get("row_count", 0),
                "failed_expectations": violations_total,
                "last_batch_id": details.get("streaming_batch_id"),
            },
        ),
        _timeline_event(
            phase="Rejects",
            title="Rejected rows copied to demo.streaming.events_rejects",
            description="The demo reject sink captures failing rows with their reason column.",
            status="warning" if reject_count else "info",
            metrics={"reject_rows": reject_count},
        ),
    ]
    return _ScenarioResult(
        dataset_name=_OUTPUT_CONTRACT,
        dataset_version=details.get("dataset_version") or dataset_version,
        validation=validation,
        queries=queries,
        dq_details=dq_details,
        timeline=timeline,
    )


def _scenario_schema_break(seconds: int, *, run_type: str) -> _ScenarioResult:
    spark = _spark_session()
    started_at = datetime.now(timezone.utc)
    dataset_version = started_at.isoformat()
    df, read_status = read_stream_with_contract(
        spark=spark,
        contract_id=_INPUT_CONTRACT,
        contract_service=contract_service,
        expected_contract_version=f"=={_CONTRACT_VERSIONS[_INPUT_CONTRACT]}",
        data_quality_service=dq_service,
        governance_service=governance_service,
        dataset_locator=StaticDatasetLocator(dataset_version=None),
        options={"rowsPerSecond": "6", "numPartitions": "1"},
    )
    broken_df = df.drop("value")
    checkpoint = _checkpoint_dir("schema_break", version=dataset_version)
    try:
        validation = write_stream_with_contract(
            df=broken_df,
            contract_id=_OUTPUT_CONTRACT,
            contract_service=contract_service,
            expected_contract_version=f"=={_CONTRACT_VERSIONS[_OUTPUT_CONTRACT]}",
            data_quality_service=dq_service,
            governance_service=governance_service,
            dataset_locator=StaticDatasetLocator(dataset_version=dataset_version),
            options={
                "checkpointLocation": str(checkpoint),
                "queryName": f"demo_stream_schema_{dataset_version}",
            },
        )
        queries = list(validation.details.get("streaming_queries", []) or [])
        try:
            _drive_queries(queries, seconds=seconds)
        finally:
            _stop_queries(queries)
        details = dict(validation.details)
        details["streaming_queries"] = _query_metadata(queries)
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
                phase="Validation",
                title="Schema mismatch detected",
                description="The outgoing stream dropped the value column so contract alignment fails and the stream halts.",
                status="danger",
                metrics={"missing_columns": ["value"], "last_batch_id": details.get("streaming_batch_id")},
            ),
        ]
        return _ScenarioResult(
            dataset_name=_OUTPUT_CONTRACT,
            dataset_version=details.get("dataset_version") or dataset_version,
            validation=validation,
            queries=queries,
            dq_details=dq_details,
            timeline=timeline,
            status_reason=status_reason,
        )
    except Exception as exc:
        dq_details = {
            "input": read_status.details if read_status else {},
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
                phase="Validation",
                title="Schema mismatch detected",
                description=str(exc),
                status="danger",
            ),
        ]
        return _ScenarioResult(
            dataset_name=_OUTPUT_CONTRACT,
            dataset_version=None,
            validation=None,
            queries=[],
            dq_details=dq_details,
            timeline=timeline,
            status_reason=str(exc),
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
) -> tuple[str, str]:
    """Execute a streaming scenario and record its outcome."""

    runner = _SCENARIO_MAP.get(scenario_key)
    if runner is None:
        raise ValueError(f"Unknown streaming scenario: {scenario_key}")
    result = runner(seconds, run_type=run_type)
    contract_version = _CONTRACT_VERSIONS.get(_OUTPUT_CONTRACT, "")
    return _record_result(
        result,
        scenario_key=scenario_key,
        run_type=run_type,
        contract_id=_OUTPUT_CONTRACT,
        contract_version=contract_version,
    )


__all__ = ["run_streaming_scenario"]
