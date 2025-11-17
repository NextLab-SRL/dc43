from __future__ import annotations

"""Spark/Databricks integration helpers.

High-level wrappers to read/write DataFrames while enforcing ODCS contracts
and coordinating with an external Data Quality client when provided.
"""

from typing import (
    Any,
    Callable,
    Dict,
    Iterable,
    List,
    Mapping,
    Optional,
    Protocol,
    Sequence,
    Tuple,
    Literal,
    Type,
    Union,
    overload,
    runtime_checkable,
)
import copy
import logging
import warnings
import tempfile
from dataclasses import dataclass, field, is_dataclass, replace
from datetime import datetime, timezone
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession

from dc43_service_clients.contracts.client.interface import ContractServiceClient
from dc43_service_clients.data_quality.client.interface import DataQualityServiceClient
from dc43_service_clients.data_quality import ObservationPayload, ValidationResult
from dc43_service_clients.data_products import (
    DataProductInputBinding,
    DataProductOutputBinding,
    DataProductServiceClient,
    normalise_input_binding,
    normalise_output_binding,
)
from dc43_service_clients.governance.client.interface import GovernanceServiceClient
from dc43_service_clients.governance import (
    PipelineContext,
    QualityAssessment,
    normalise_pipeline_context,
    GovernancePublicationMode,
    resolve_publication_mode,
)
from dc43_service_clients.governance.models import (
    GovernanceReadContext,
    GovernanceWriteContext,
    ResolvedReadPlan,
    ResolvedWritePlan,
)
from .data_quality import (
    build_metrics_payload,
    collect_observations,
)
from .open_data_lineage import build_lineage_run_event
from .open_telemetry import record_telemetry_span
from .validation import apply_contract
from dc43_service_backends.core.odcs import contract_identity, custom_properties_dict, ensure_version
from dc43_service_backends.core.versioning import SemVer, version_key
from open_data_contract_standard.model import OpenDataContractStandard, Server  # type: ignore
from dc43_service_clients.odps import OpenDataProductStandard

from .violation_strategy import (
    NoOpWriteViolationStrategy,
    WriteRequest,
    WriteStrategyContext,
    WriteViolationStrategy,
)


PipelineContextLike = Union[
    PipelineContext,
    Mapping[str, object],
    Sequence[tuple[str, object]],
    str,
]


@runtime_checkable
class SupportsContractStatusValidation(Protocol):
    """Expose a contract-status validation hook."""

    def validate_contract_status(
        self,
        *,
        contract: OpenDataContractStandard,
        enforce: bool,
        operation: str,
    ) -> None:
        ...


@runtime_checkable
class SupportsDataProductStatusValidation(Protocol):
    """Expose a data-product status validation hook."""

    def validate_data_product_status(
        self,
        *,
        data_product: OpenDataProductStandard,
        enforce: bool,
        operation: str,
    ) -> None:
        ...


@runtime_checkable
class SupportsDataProductStatusPolicy(Protocol):
    """Expose data product status policy attributes."""

    allowed_data_product_statuses: Sequence[str]
    allow_missing_data_product_status: bool
    data_product_status_case_insensitive: bool
    data_product_status_failure_message: str | None


@dataclass(slots=True)
class GovernanceSparkReadRequest:
    """Wrapper aggregating governance context and Spark-specific overrides."""

    context: GovernanceReadContext | Mapping[str, object]
    format: Optional[str] = None
    path: Optional[str] = None
    table: Optional[str] = None
    options: Optional[Mapping[str, str]] = None
    dataset_locator: Optional["DatasetLocatorStrategy"] = None
    status_strategy: Optional["ReadStatusStrategy"] = None
    pipeline_context: Optional[PipelineContextLike] = None
    publication_mode: GovernancePublicationMode | str | None = None

    def __post_init__(self) -> None:
        if not isinstance(self.context, GovernanceReadContext):
            if isinstance(self.context, Mapping):
                self.context = GovernanceReadContext(**dict(self.context))
            else:
                raise TypeError("context must be a GovernanceReadContext or mapping")
        if self.options is not None and not isinstance(self.options, dict):
            self.options = dict(self.options)
        if isinstance(self.publication_mode, str):
            self.publication_mode = GovernancePublicationMode.from_value(self.publication_mode)
        if self.pipeline_context is not None:
            self.context.pipeline_context = self.pipeline_context


@dataclass(slots=True)
class GovernanceSparkWriteRequest:
    """Wrapper aggregating governance context and Spark write overrides."""

    context: GovernanceWriteContext | Mapping[str, object]
    format: Optional[str] = None
    path: Optional[str] = None
    table: Optional[str] = None
    options: Optional[Mapping[str, str]] = None
    mode: str = "append"
    dataset_locator: Optional["DatasetLocatorStrategy"] = None
    pipeline_context: Optional[PipelineContextLike] = None
    publication_mode: GovernancePublicationMode | str | None = None

    def __post_init__(self) -> None:
        if not isinstance(self.context, GovernanceWriteContext):
            if isinstance(self.context, Mapping):
                self.context = GovernanceWriteContext(**dict(self.context))
            else:
                raise TypeError("context must be a GovernanceWriteContext or mapping")
        if self.options is not None and not isinstance(self.options, dict):
            self.options = dict(self.options)
        if isinstance(self.publication_mode, str):
            self.publication_mode = GovernancePublicationMode.from_value(self.publication_mode)
        if self.pipeline_context is not None:
            self.context.pipeline_context = self.pipeline_context

def _evaluate_with_service(
    *,
    contract: OpenDataContractStandard,
    service: DataQualityServiceClient,
    schema: Mapping[str, Mapping[str, Any]] | None = None,
    metrics: Mapping[str, Any] | None = None,
    reused: bool = False,
) -> ValidationResult:
    """Evaluate ``contract`` observations through ``service``."""

    payload = ObservationPayload(
        metrics=dict(metrics or {}),
        schema=dict(schema) if schema else None,
        reused=reused,
    )
    result = service.evaluate(contract=contract, payload=payload)
    if schema and not result.schema:
        result.schema = dict(schema)
    if metrics and not result.metrics:
        result.metrics = dict(metrics)
    return result


def _merge_pipeline_context(
    base: Optional[Mapping[str, Any]],
    extra: Optional[Mapping[str, Any]],
) -> Optional[Dict[str, Any]]:
    """Combine two pipeline context mappings."""

    combined: Dict[str, Any] = {}
    if base:
        combined.update(base)
    if extra:
        combined.update(extra)
    return combined or None


_OBSERVATION_SCOPE_LABELS: Dict[str, str] = {
    "input_slice": "Governed read snapshot",
    "pre_write_dataframe": "Pre-write dataframe snapshot",
    "streaming_batch": "Streaming micro-batch snapshot",
}


def _annotate_observation_scope(
    result: Optional[ValidationResult],
    *,
    operation: str,
    scope: str,
) -> None:
    """Attach observation metadata to ``result`` for downstream consumers."""

    if result is None:
        return
    payload: Dict[str, Any] = {
        "observation_operation": operation,
        "observation_scope": scope,
    }
    label = _OBSERVATION_SCOPE_LABELS.get(scope)
    if label:
        payload["observation_label"] = label
    result.merge_details(payload)


def get_delta_version(
    spark: SparkSession,
    *,
    table: Optional[str] = None,
    path: Optional[str] = None,
) -> Optional[str]:
    """Return the latest Delta table version as a string if available."""

    try:
        ref = table if table else f"delta.`{path}`"
        row = spark.sql(f"DESCRIBE HISTORY {ref} LIMIT 1").head(1)
        if not row:
            return None
        # versions column name can be 'version'
        v = row[0][0]
        return str(v)
    except Exception:
        return None


def _normalise_path_ref(path: Optional[str | Iterable[str]]) -> Optional[str]:
    """Return a representative path from ``path``.

    Readers may receive an iterable of concrete paths when a contract describes
    cumulative layouts (for example, delta-style incremental folders).  For
    dataset identifiers and compatibility checks we fall back to the first
    element so downstream logic keeps working with a stable reference.
    """

    if path is None:
        return None
    if isinstance(path, (list, tuple, set)):
        for item in path:
            return str(item)
        return None
    return path


def _supports_dataframe_checkpointing(df: DataFrame) -> bool:
    """Return ``True`` when the active Spark cluster supports checkpointing."""

    try:
        spark = df.sparkSession
    except Exception:  # pragma: no cover - defensive, matches write path guard
        return True

    try:
        conf = spark.sparkContext.getConf()
    except Exception:  # pragma: no cover - fallback to legacy behaviour
        return True

    indicators: tuple[tuple[str, tuple[str, ...]], ...] = (
        ("spark.databricks.service.serverless.enabled", ("true", "1", "yes")),
        ("spark.databricks.service.serverless", ("true", "1", "yes")),
        ("spark.databricks.service.clusterSource", ("serverless",)),
        ("spark.databricks.clusterUsageTags.clusterAllType", ("serverless",)),
    )

    for key, matches in indicators:
        try:
            raw_value = conf.get(key, "")
        except Exception:  # pragma: no cover - SparkConf guards can raise
            raw_value = ""
        value = str(raw_value).lower()
        if value and any(match in value for match in matches):
            return False

    return True


def _looks_like_table_reference(value: str) -> bool:
    """Return ``True`` when ``value`` resembles a table identifier."""

    if "://" in value:
        return False
    if any(sep in value for sep in ("/", "\\")):
        return False
    return "." in value


def _promote_delta_path_to_table(
    *,
    path: Optional[str],
    table: Optional[str],
    format: Optional[str],
    spark: Optional[SparkSession] = None,
) -> tuple[Optional[str], Optional[str]]:
    """Return adjusted ``(path, table)`` when Delta references point to tables."""

    if table is not None or not isinstance(path, str) or not _looks_like_table_reference(path):
        return path, table

    if (format or "").lower() == "delta":
        return None, path

    if spark is not None:
        try:
            catalog = spark.catalog
        except AttributeError:
            catalog = None
        if catalog is not None:
            try:
                if catalog.tableExists(path):
                    return None, path
            except Exception:  # pragma: no cover - Spark catalog guards
                pass

    return path, table


def dataset_id_from_ref(*, table: Optional[str] = None, path: Optional[str | Iterable[str]] = None) -> str:
    """Build a dataset id from a table name or path (``table:...``/``path:...``)."""

    if table:
        return f"table:{table}"
    normalised = _normalise_path_ref(path)
    if normalised:
        return f"path:{normalised}"
    return "unknown"


def _safe_fs_name(value: str) -> str:
    """Return a filesystem-safe representation of ``value``."""

    return "".join(ch if ch.isalnum() or ch in ("_", "-", ".") else "_" for ch in value)


def _derive_metrics_checkpoint(
    base: Optional[str],
    dataset_id: Optional[str],
    dataset_version: Optional[str],
) -> str:
    """Return a checkpoint path for streaming metric collectors."""

    if isinstance(base, str) and base:
        trimmed = base.rstrip("/")
        if trimmed.endswith("_dq"):
            return trimmed
        return f"{trimmed}_dq"

    safe_id = _safe_fs_name(dataset_id or "stream")
    safe_version = _safe_fs_name(dataset_version or _timestamp())
    root = Path(tempfile.gettempdir()) / "dc43_stream_metrics" / safe_id / safe_version
    try:
        root.mkdir(parents=True, exist_ok=True)
    except Exception:  # pragma: no cover - filesystem may be managed by Spark
        pass
    return str(root)


class StreamingInterventionError(RuntimeError):
    """Raised when a streaming intervention strategy blocks the pipeline."""


@dataclass(frozen=True)
class StreamingInterventionContext:
    """Information provided to intervention strategies for each micro-batch."""

    batch_id: int
    validation: ValidationResult
    dataset_id: str
    dataset_version: str


class StreamingInterventionStrategy(Protocol):
    """Decide whether a streaming pipeline should be interrupted."""

    def decide(self, context: StreamingInterventionContext) -> Optional[str]:
        """Return a reason to block the stream or ``None`` to continue."""


class NoOpStreamingInterventionStrategy:
    """Default strategy that never blocks the streaming pipeline."""

    def decide(self, context: StreamingInterventionContext) -> Optional[str]:  # noqa: D401 - short description
        return None


class StreamingObservationWriter:
    """Send streaming micro-batch observations to the data-quality service."""

    def __init__(
        self,
        *,
        contract: OpenDataContractStandard,
        expectation_plan: Sequence[Mapping[str, Any]],
        data_quality_service: DataQualityServiceClient,
        dataset_id: Optional[str],
        dataset_version: Optional[str],
        enforce: bool,
        checkpoint_location: Optional[str] = None,
        intervention: Optional[StreamingInterventionStrategy] = None,
        progress_callback: Optional[Callable[[Mapping[str, Any]], None]] = None,
    ) -> None:
        self.contract = contract
        self.expectation_plan = list(expectation_plan)
        self.data_quality_service = data_quality_service
        self.dataset_id = dataset_id or "unknown"
        self.dataset_version = dataset_version or "unknown"
        self.enforce = enforce
        self._validation: Optional[ValidationResult] = None
        self._latest_batch_id: Optional[int] = None
        self._active = False
        self._checkpoint_location = _derive_metrics_checkpoint(
            checkpoint_location,
            self.dataset_id,
            self.dataset_version,
        )
        default_name = f"dc43_metrics_{_safe_fs_name(self.dataset_id)}"
        self.query_name = f"{default_name}_{_safe_fs_name(self.dataset_version)}"
        self._intervention = intervention or NoOpStreamingInterventionStrategy()
        self._batches: List[Dict[str, Any]] = []
        self._progress_callback = progress_callback
        self._sink_queries: List[Any] = []

    @property
    def checkpoint_location(self) -> str:
        """Location used to checkpoint the metrics query."""

        return self._checkpoint_location

    @property
    def active(self) -> bool:
        """Whether the observation writer has already started its query."""

        return self._active

    def attach_validation(self, validation: ValidationResult) -> None:
        """Attach the validation object that should receive streaming metrics."""

        if self._validation is not None and self._validation is not validation:
            raise RuntimeError("StreamingObservationWriter already bound to a validation")

        self._validation = validation
        validation.merge_details(
            {
                "dataset_id": self.dataset_id,
                "dataset_version": self.dataset_version,
            }
        )

    def latest_validation(self) -> Optional[ValidationResult]:
        """Return the most recent validation produced by the observer."""

        return self._validation

    def streaming_batches(self) -> List[Mapping[str, Any]]:
        """Return the recorded micro-batch timeline."""

        return [dict(item) for item in self._batches]

    def _record_batch(
        self,
        *,
        batch_id: int,
        metrics: Mapping[str, Any] | None,
        row_count: int,
        status: str,
        timestamp: datetime,
        errors: Optional[Sequence[str]] = None,
        warnings: Optional[Sequence[str]] = None,
        intervention: Optional[str] = None,
    ) -> None:
        metrics_map = dict(metrics or {})
        violation_total = sum(
            int(value)
            for key, value in metrics_map.items()
            if key.startswith("violations.") and isinstance(value, (int, float))
        )
        entry: Dict[str, Any] = {
            "batch_id": batch_id,
            "timestamp": timestamp.isoformat(),
            "row_count": row_count,
            "violations": violation_total,
            "status": status,
        }
        if metrics_map:
            entry["metrics"] = metrics_map
        if errors:
            entry["errors"] = list(errors)
        if warnings:
            entry["warnings"] = list(warnings)
        if intervention:
            entry["intervention"] = intervention
        self._batches.append(entry)
        self._notify_progress({"type": "batch", **entry})

    def _notify_progress(self, event: Mapping[str, Any]) -> None:
        if self._progress_callback is None:
            return
        try:
            self._progress_callback(dict(event))
        except Exception:  # pragma: no cover - best effort progress hook
            logger.exception("Streaming progress callback failed")

    def watch_sink_query(self, query: Any) -> None:
        """Track a sink query so it can be stopped on enforcement failure."""

        if query not in self._sink_queries:
            self._sink_queries.append(query)

    def _stop_sink_queries(self) -> None:
        for query in list(self._sink_queries):
            try:
                stop = query.stop  # type: ignore[attr-defined]
            except AttributeError:
                stop = None
            try:
                if callable(stop):
                    stop()
            except Exception:  # pragma: no cover - best effort cleanup
                logger.exception("Failed to stop streaming sink query")

    def _merge_batch_details(
        self,
        result: ValidationResult,
        *,
        batch_id: int,
    ) -> None:
        details = {
            "dataset_id": self.dataset_id,
            "dataset_version": self.dataset_version,
            "streaming_batch_id": batch_id,
        }
        if result.metrics:
            details["streaming_metrics"] = dict(result.metrics)
        if self._batches:
            details["streaming_batches"] = [dict(item) for item in self._batches]
        result.merge_details(details)
        if self._validation is not None:
            validation = self._validation
            validation.ok = result.ok
            validation.errors = list(result.errors)
            validation.warnings = list(result.warnings)
            validation.metrics = dict(result.metrics)
            validation.schema = dict(result.schema)
            validation.status = result.status
            validation.reason = result.reason
            validation.merge_details(details)
            self._validation = validation
        else:
            self._validation = result

    def process_batch(self, batch_df: DataFrame, batch_id: int) -> ValidationResult:
        """Validate a micro-batch and update the attached validation."""

        timestamp = datetime.now(timezone.utc)
        schema, metrics = collect_observations(
            batch_df,
            self.contract,
            expectations=self.expectation_plan,
            collect_metrics=True,
        )
        row_count = metrics.get("row_count")
        if isinstance(row_count, (int, float)) and row_count <= 0:
            logger.debug(
                "Skipping empty streaming batch %s for %s@%s",
                batch_id,
                self.dataset_id,
                self.dataset_version,
            )
            self._latest_batch_id = batch_id
            validation = self._validation
            if validation is None:
                validation = ValidationResult(ok=True, errors=[], warnings=[])
            validation.merge_details(
                {
                    "dataset_id": self.dataset_id,
                    "dataset_version": self.dataset_version,
                    "streaming_batch_id": batch_id,
                }
            )
            self._validation = validation
            self._record_batch(
                batch_id=batch_id,
                metrics={},
                row_count=0,
                status="idle",
                timestamp=timestamp,
            )
            return validation

        if self.data_quality_service is not None:
            result = _evaluate_with_service(
                contract=self.contract,
                service=self.data_quality_service,
                schema=schema,
                metrics=metrics,
                reused=False,
            )
        else:
            result = self._evaluate_without_service(schema=schema, metrics=metrics)
        self._latest_batch_id = batch_id
        status = "ok"
        if result.errors:
            status = "error"
        elif result.warnings:
            status = "warning"
        self._record_batch(
            batch_id=batch_id,
            metrics=result.metrics or metrics,
            row_count=int(metrics.get("row_count", 0) or 0),
            status=status,
            timestamp=timestamp,
            errors=result.errors if result.errors else None,
            warnings=result.warnings if result.warnings else None,
        )
        self._merge_batch_details(result, batch_id=batch_id)

        if self.enforce and not result.ok:
            self._stop_sink_queries()
            raise ValueError(
                "Streaming batch %s failed data-quality validation: %s"
                % (batch_id, result.errors)
            )

        decision = self._intervention.decide(
            StreamingInterventionContext(
                batch_id=batch_id,
                validation=result,
                dataset_id=self.dataset_id,
                dataset_version=self.dataset_version,
            )
        )
        if decision:
            if self._batches:
                self._batches[-1]["intervention"] = decision
            batches_payload = [dict(item) for item in self._batches]
            if self._validation is not None:
                self._validation.merge_details({"streaming_batches": batches_payload})
            result.merge_details({"streaming_batches": batches_payload})
            reason_details = {"streaming_intervention_reason": decision}
            if self._validation is not None:
                self._validation.merge_details(reason_details)
            result.merge_details(reason_details)
            self._notify_progress(
                {
                    "type": "intervention",
                    "batch_id": batch_id,
                    "reason": decision,
                }
            )
            self._stop_sink_queries()
            raise StreamingInterventionError(decision)

        return result

    def start(self, df: DataFrame, *, output_mode: str) -> "StreamingQuery":
        """Start the observation writer for ``df`` and return its query handle."""

        if self._active:
            raise RuntimeError("StreamingObservationWriter can only be started once")
        self._active = True

        def _run(batch_df: DataFrame, batch_id: int) -> None:
            self.process_batch(batch_df, batch_id)

        writer = df.writeStream.foreachBatch(_run).outputMode(output_mode)
        writer = writer.option("checkpointLocation", self.checkpoint_location)
        if self.query_name:
            writer = writer.queryName(self.query_name)
        query = writer.start()
        try:
            query_name = query.name  # type: ignore[attr-defined]
        except AttributeError:
            query_name = self.query_name
        try:
            query_id = query.id  # type: ignore[attr-defined]
        except AttributeError:
            query_id = ""
        self._notify_progress(
            {
                "type": "observer-started",
                "query_name": query_name,
                "id": query_id,
            }
        )
        return query

    def _evaluate_without_service(
        self,
        *,
        schema: Mapping[str, Mapping[str, Any]],
        metrics: Mapping[str, Any],
    ) -> ValidationResult:
        """Return a validation outcome without a dedicated DQ service."""

        violation_counts: Dict[str, float] = {}
        for key, value in metrics.items():
            if not isinstance(key, str) or not key.startswith("violations."):
                continue
            suffix = key.partition(".")[2]
            try:
                violation_counts[suffix] = float(value)
            except (TypeError, ValueError):
                continue

        errors: list[str] = []
        warnings: list[str] = []
        for descriptor in self.expectation_plan:
            if not isinstance(descriptor, Mapping):
                continue
            key = descriptor.get("key")
            if not isinstance(key, str):
                continue
            count = violation_counts.get(key, 0.0)
            if count <= 0:
                continue
            message = f"Expectation {key} reported {int(count)} violation(s)"
            if bool(descriptor.get("optional")):
                warnings.append(message)
            else:
                errors.append(message)

        ok = not errors
        status = "ok"
        if errors:
            status = "block"
        elif warnings:
            status = "warn"

        return ValidationResult(
            ok=ok,
            errors=errors,
            warnings=warnings,
            metrics=dict(metrics),
            schema=dict(schema),
            status=status,
        )

logger = logging.getLogger(__name__)


def _warn_deprecated(old: str, new: str) -> None:
    """Emit a deprecation warning for legacy helpers."""

    warnings.warn(
        f"{old} is deprecated and will be removed in a future release; use {new} instead.",
        DeprecationWarning,
        stacklevel=2,
    )


def _as_governance_service(
    service: Optional[GovernanceServiceClient],
) -> Optional[GovernanceServiceClient]:
    """Return the provided governance service when configured."""

    return service
@dataclass
class DatasetResolution:
    """Resolved location and governance identifiers for a dataset."""

    path: Optional[str]
    table: Optional[str]
    format: Optional[str]
    dataset_id: Optional[str]
    dataset_version: Optional[str]
    read_options: Optional[Dict[str, str]] = None
    write_options: Optional[Dict[str, str]] = None
    custom_properties: Optional[Dict[str, Any]] = None
    load_paths: Optional[List[str]] = None


class DatasetLocatorStrategy(Protocol):
    """Resolve IO coordinates and identifiers for read/write operations."""

    def for_read(
        self,
        *,
        contract: Optional[OpenDataContractStandard],
        spark: SparkSession,
        format: Optional[str],
        path: Optional[str],
        table: Optional[str],
    ) -> DatasetResolution:
        ...

    def for_write(
        self,
        *,
        contract: Optional[OpenDataContractStandard],
        df: DataFrame,
        format: Optional[str],
        path: Optional[str],
        table: Optional[str],
    ) -> DatasetResolution:
        ...


def _timestamp() -> str:
    """Return an ISO timestamp suitable for dataset versioning."""

    now = datetime.utcnow().replace(tzinfo=timezone.utc)
    return now.isoformat().replace("+00:00", "Z")


@dataclass
class ContractFirstDatasetLocator:
    """Default locator that favours contract servers over provided hints."""

    clock: Callable[[], str] = _timestamp

    def _resolve_base(
        self,
        contract: Optional[OpenDataContractStandard],
        *,
        path: Optional[str],
        table: Optional[str],
        format: Optional[str],
        spark: Optional[SparkSession] = None,
    ) -> tuple[Optional[str], Optional[str], Optional[str], Optional[Server]]:
        server: Optional[Server] = None
        if contract and contract.servers:
            c_path, c_table = _ref_from_contract(contract)
            server = contract.servers[0]
            if server is not None:
                try:
                    c_format = server.format  # type: ignore[attr-defined]
                except AttributeError:
                    c_format = None
            else:
                c_format = None
            if c_path is not None:
                path = c_path
            if c_table is not None:
                table = c_table
            if c_format is not None and format is None:
                format = c_format
        path, table = _promote_delta_path_to_table(
            path=path,
            table=table,
            format=format,
            spark=spark,
        )
        return path, table, format, server

    def _resolution(
        self,
        contract: Optional[OpenDataContractStandard],
        *,
        path: Optional[str],
        table: Optional[str],
        format: Optional[str],
        include_timestamp: bool,
    ) -> DatasetResolution:
        dataset_id = contract.id if contract else dataset_id_from_ref(table=table, path=path)
        dataset_version = self.clock() if include_timestamp else None
        server_props: Optional[Dict[str, Any]] = None
        read_options: Optional[Dict[str, str]] = None
        write_options: Optional[Dict[str, str]] = None
        if contract and contract.servers:
            first = contract.servers[0]
            props = custom_properties_dict(first)
            if props:
                server_props = props
                versioning = props.get(ContractVersionLocator.VERSIONING_PROPERTY)
                if isinstance(versioning, Mapping):
                    read_map = versioning.get("readOptions")
                    if isinstance(read_map, Mapping):
                        read_options = {
                            str(k): str(v)
                            for k, v in read_map.items()
                            if v is not None
                        }
                    write_map = versioning.get("writeOptions")
                    if isinstance(write_map, Mapping):
                        write_options = {
                            str(k): str(v)
                            for k, v in write_map.items()
                            if v is not None
                        }
        return DatasetResolution(
            path=path,
            table=table,
            format=format,
            dataset_id=dataset_id,
            dataset_version=dataset_version,
            read_options=read_options,
            write_options=write_options,
            custom_properties=server_props,
            load_paths=None,
        )

    def for_read(
        self,
        *,
        contract: Optional[OpenDataContractStandard],
        spark: SparkSession,
        format: Optional[str],
        path: Optional[str],
        table: Optional[str],
    ) -> DatasetResolution:  # noqa: D401 - short docstring
        path, table, format, _ = self._resolve_base(
            contract,
            path=path,
            table=table,
            format=format,
            spark=spark,
        )
        return self._resolution(
            contract,
            path=path,
            table=table,
            format=format,
            include_timestamp=False,
        )

    def for_write(
        self,
        *,
        contract: Optional[OpenDataContractStandard],
        df: DataFrame,
        format: Optional[str],
        path: Optional[str],
        table: Optional[str],
    ) -> DatasetResolution:  # noqa: D401 - short docstring
        path, table, format, _ = self._resolve_base(
            contract,
            path=path,
            table=table,
            format=format,
            spark=None,
        )
        return self._resolution(
            contract,
            path=path,
            table=table,
            format=format,
            include_timestamp=True,
        )


@dataclass
class StaticDatasetLocator:
    """Locator overriding specific fields while delegating to a base strategy."""

    dataset_id: Optional[str] = None
    dataset_version: Optional[str] = None
    path: Optional[str] = None
    table: Optional[str] = None
    format: Optional[str] = None
    base: DatasetLocatorStrategy = field(default_factory=ContractFirstDatasetLocator)

    def _merge(self, resolution: DatasetResolution) -> DatasetResolution:
        return DatasetResolution(
            path=self.path or resolution.path,
            table=self.table or resolution.table,
            format=self.format or resolution.format,
            dataset_id=self.dataset_id or resolution.dataset_id,
            dataset_version=self.dataset_version or resolution.dataset_version,
            read_options=dict(resolution.read_options or {}),
            write_options=dict(resolution.write_options or {}),
            custom_properties=resolution.custom_properties,
            load_paths=list(resolution.load_paths or []),
        )

    def for_read(
        self,
        *,
        contract: Optional[OpenDataContractStandard],
        spark: SparkSession,
        format: Optional[str],
        path: Optional[str],
        table: Optional[str],
    ) -> DatasetResolution:  # noqa: D401 - short docstring
        base_resolution = self.base.for_read(
            contract=contract,
            spark=spark,
            format=format,
            path=path,
            table=table,
        )
        return self._merge(base_resolution)

    def for_write(
        self,
        *,
        contract: Optional[OpenDataContractStandard],
        df: DataFrame,
        format: Optional[str],
        path: Optional[str],
        table: Optional[str],
    ) -> DatasetResolution:  # noqa: D401 - short docstring
        base_resolution = self.base.for_write(
            contract=contract,
            df=df,
            format=format,
            path=path,
            table=table,
        )
        return self._merge(base_resolution)


@dataclass
class ContractVersionLocator:
    """Locator that appends a version directory or time-travel hint."""

    dataset_version: str
    dataset_id: Optional[str] = None
    subpath: Optional[str] = None
    base: DatasetLocatorStrategy = field(default_factory=ContractFirstDatasetLocator)

    VERSIONING_PROPERTY = "dc43.core.versioning"

    @staticmethod
    def _version_key(value: str) -> tuple[int, Tuple[int, int, int] | float | str, str]:
        candidate = value
        if candidate.endswith("Z"):
            candidate = candidate[:-1] + "+00:00"
        try:
            dt = datetime.fromisoformat(candidate)
            return (0, dt.timestamp(), value)
        except ValueError:
            pass
        try:
            parsed = SemVer.parse(value)
            return (1, (parsed.major, parsed.minor, parsed.patch), value)
        except ValueError:
            return (2, value, value)

    @classmethod
    def _sorted_versions(cls, entries: Iterable[str]) -> List[str]:
        return sorted(entries, key=lambda item: cls._version_key(item))

    @staticmethod
    @staticmethod
    def _render_template(template: str, *, version_value: str, safe_value: str) -> str:
        return (
            template.replace("{version}", version_value)
            .replace("{safeVersion}", safe_value)
        )

    @staticmethod
    def _folder_version_value(path: Path) -> str:
        marker = path / ".dc43_version"
        if marker.exists():
            try:
                text = marker.read_text().strip()
            except OSError:
                text = ""
            if text:
                return text
        return path.name

    @classmethod
    def _versioning_config(cls, resolution: DatasetResolution) -> Optional[Mapping[str, Any]]:
        props = resolution.custom_properties or {}
        value = props.get(cls.VERSIONING_PROPERTY)
        if isinstance(value, Mapping):
            return value
        return None

    @classmethod
    def _expand_versioning_paths(
        cls,
        resolution: DatasetResolution,
        *,
        base_path: Optional[str],
        dataset_version: Optional[str],
    ) -> tuple[Optional[List[str]], Dict[str, str]]:
        config = cls._versioning_config(resolution)
        if not config or not base_path or not dataset_version:
            return None, {}

        base = Path(base_path)
        base_dir = base.parent if base.suffix else base
        if not base_dir.exists():
            return None, {}

        include_prior = bool(config.get("includePriorVersions"))
        folder_template = str(config.get("subfolder", "{version}"))
        file_pattern = config.get("filePattern")
        if file_pattern is not None:
            file_pattern = str(file_pattern)
        elif base.suffix:
            file_pattern = base.name

        dataset_version_normalised = dataset_version
        lower = dataset_version.lower()
        entries: List[tuple[str, str]] = []
        try:
            for entry in base_dir.iterdir():
                if not entry.is_dir():
                    continue
                display = cls._folder_version_value(entry)
                entries.append((display, entry.name))
        except FileNotFoundError:
            return None, {}
        if not entries:
            return None, {}
        entries.sort(key=lambda item: cls._version_key(item[0]))

        selected: List[tuple[str, str]] = []
        if lower == "latest":
            alias_key = None
            alias_path = base_dir / dataset_version_normalised
            if alias_path.exists():
                try:
                    resolved_alias = alias_path.resolve()
                except OSError:
                    resolved_alias = alias_path
                if resolved_alias.is_dir():
                    alias_display = cls._folder_version_value(resolved_alias)
                    alias_key = cls._version_key(alias_display)

            if include_prior:
                if alias_key is not None:
                    selected = [
                        entry for entry in entries if cls._version_key(entry[0]) <= alias_key
                    ]
                else:
                    selected = entries
            elif entries:
                if alias_key is not None:
                    selected = [
                        entry for entry in entries if cls._version_key(entry[0]) == alias_key
                    ]
                    if not selected and entries:
                        selected = [entries[-1]]
                else:
                    selected = [entries[-1]]
        else:
            target_key = cls._version_key(dataset_version_normalised)
            eligible = [entry for entry in entries if cls._version_key(entry[0]) <= target_key]
            alias_like = "__" in dataset_version_normalised
            effective_include_prior = include_prior and not alias_like
            if effective_include_prior:
                selected = eligible
            else:
                exact = next((entry for entry in entries if entry[0] == dataset_version_normalised), None)
                if exact:
                    selected = [exact]
                else:
                    safe_candidate = _safe_fs_name(dataset_version_normalised)
                    fallback = next((entry for entry in entries if entry[1] == safe_candidate), None)
                    if fallback:
                        selected = [fallback]
                    elif eligible:
                        selected = [eligible[-1]]

        if not selected:
            candidate_path = base_dir / dataset_version_normalised
            if candidate_path.exists():
                selected = [(dataset_version_normalised, candidate_path.name)]
            else:
                return None, {}

        resolved_paths: List[str] = []
        for display_value, folder_name in selected:
            rendered_folder = cls._render_template(
                folder_template,
                version_value=display_value,
                safe_value=folder_name,
            )
            root = base_dir / rendered_folder if rendered_folder else base_dir
            if not root.exists():
                fallback_root = base_dir / folder_name
                if fallback_root.exists():
                    root = fallback_root
            if file_pattern:
                pattern = cls._render_template(
                    file_pattern,
                    version_value=display_value,
                    safe_value=folder_name,
                )
                matches = list(root.glob(pattern))
                if matches:
                    resolved_paths.extend(str(path) for path in matches)
            else:
                if root.exists():
                    resolved_paths.append(str(root))

        read_opts: Dict[str, str] = {}
        extra_read = config.get("readOptions")
        if isinstance(extra_read, Mapping):
            for k, v in extra_read.items():
                if isinstance(v, bool):
                    read_opts[str(k)] = str(v).lower()
                else:
                    read_opts[str(k)] = str(v)

        return (resolved_paths or None), read_opts

    def _resolve_path(self, resolution: DatasetResolution) -> Optional[str]:
        path = resolution.path
        if not path:
            return None

        fmt = (resolution.format or "").lower()
        if fmt == "delta":
            return path

        base = Path(path)
        safe_component: Optional[str] = None
        if self.dataset_version:
            candidate = _safe_fs_name(self.dataset_version)
            if candidate and candidate != self.dataset_version:
                safe_component = candidate

        if base.suffix:
            version_component = self.dataset_version
            parent = base.parent / base.stem
            if safe_component and version_component:
                preferred_dir = parent / version_component
                if not preferred_dir.exists():
                    version_component = safe_component
            elif safe_component and not version_component:
                version_component = safe_component

            folder = parent / version_component if version_component else parent
            if self.subpath:
                folder = folder / self.subpath
            target = folder / base.name
            return str(target)

        version_component = self.dataset_version
        if safe_component and version_component:
            preferred_dir = base / version_component
            if not preferred_dir.exists():
                version_component = safe_component
        elif safe_component and not version_component:
            version_component = safe_component

        folder = base / version_component if version_component else base
        if self.subpath:
            folder = folder / self.subpath
        return str(folder)

    @staticmethod
    def _delta_time_travel_option(dataset_version: Optional[str]) -> Optional[tuple[str, str]]:
        if not dataset_version:
            return None

        version = dataset_version.strip()
        if not version or version.lower() == "latest":
            return None

        if version.isdigit():
            return "versionAsOf", version

        candidate = version
        if candidate.endswith("Z"):
            candidate = candidate[:-1] + "+00:00"
        try:
            datetime.fromisoformat(candidate)
        except ValueError:
            return None
        return "timestampAsOf", version

    def _merge(
        self,
        contract: Optional[OpenDataContractStandard],
        resolution: DatasetResolution,
    ) -> DatasetResolution:
        resolved_path = self._resolve_path(resolution)
        dataset_id = self.dataset_id or resolution.dataset_id
        if dataset_id is None and contract is not None:
            dataset_id = contract.id
        read_options = dict(resolution.read_options or {})
        write_options = dict(resolution.write_options or {})
        load_paths = list(resolution.load_paths or [])
        base_path_hint = resolution.path
        version_paths, extra_read_options = self._expand_versioning_paths(
            resolution,
            base_path=base_path_hint,
            dataset_version=self.dataset_version,
        )
        if version_paths:
            load_paths = version_paths
            resolved_path = base_path_hint or resolved_path
        if extra_read_options:
            read_options.update(extra_read_options)
        if (resolution.format or "").lower() == "delta":
            option = self._delta_time_travel_option(self.dataset_version)
            if option:
                read_options.setdefault(*option)
        return DatasetResolution(
            path=resolved_path or resolution.path,
            table=resolution.table,
            format=resolution.format,
            dataset_id=dataset_id,
            dataset_version=self.dataset_version,
            read_options=read_options or None,
            write_options=write_options or None,
            custom_properties=resolution.custom_properties,
            load_paths=load_paths or None,
        )

    def for_read(
        self,
        *,
        contract: Optional[OpenDataContractStandard],
        spark: SparkSession,
        format: Optional[str],
        path: Optional[str],
        table: Optional[str],
    ) -> DatasetResolution:  # noqa: D401 - short docstring
        base_resolution = self.base.for_read(
            contract=contract,
            spark=spark,
            format=format,
            path=path,
            table=table,
        )
        return self._merge(contract, base_resolution)

    def for_write(
        self,
        *,
        contract: Optional[OpenDataContractStandard],
        df: DataFrame,
        format: Optional[str],
        path: Optional[str],
        table: Optional[str],
    ) -> DatasetResolution:  # noqa: D401 - short docstring
        base_resolution = self.base.for_write(
            contract=contract,
            df=df,
            format=format,
            path=path,
            table=table,
        )
        return self._merge(contract, base_resolution)


@dataclass
class ReadStatusContext:
    """Information exposed to read status strategies."""

    contract: Optional[OpenDataContractStandard]
    dataset_id: Optional[str]
    dataset_version: Optional[str]


class ReadStatusStrategy(Protocol):
    """Allow callers to react to DQ statuses before returning a dataframe."""

    def apply(
        self,
        *,
        dataframe: DataFrame,
        status: Optional[ValidationResult],
        enforce: bool,
        context: ReadStatusContext,
    ) -> tuple[DataFrame, Optional[ValidationResult]]:
        ...


@dataclass
class DefaultReadStatusStrategy:
    """Default behaviour preserving enforcement semantics."""

    allowed_contract_statuses: tuple[str, ...] = ("active",)
    allow_missing_contract_status: bool = True
    contract_status_case_insensitive: bool = True
    contract_status_failure_message: str | None = None
    allowed_data_product_statuses: tuple[str, ...] = ("active",)
    allow_missing_data_product_status: bool = True
    data_product_status_case_insensitive: bool = True
    data_product_status_failure_message: str | None = None

    def validate_contract_status(
        self,
        *,
        contract: OpenDataContractStandard,
        enforce: bool,
        operation: str,
    ) -> None:
        _validate_contract_status(
            contract=contract,
            enforce=enforce,
            operation=operation,
            allowed_statuses=self.allowed_contract_statuses,
            allow_missing=self.allow_missing_contract_status,
            case_insensitive=self.contract_status_case_insensitive,
            failure_message=self.contract_status_failure_message,
        )

    def apply(
        self,
        *,
        dataframe: DataFrame,
        status: Optional[ValidationResult],
        enforce: bool,
        context: ReadStatusContext,
    ) -> tuple[DataFrame, Optional[ValidationResult]]:  # noqa: D401 - short docstring
        contract = context.contract
        if contract is not None:
            self.validate_contract_status(
                contract=contract,
                enforce=enforce,
                operation="read",
            )
        if enforce and status and status.status == "block":
            raise ValueError(f"DQ status is blocking: {status.reason or status.details}")
        return dataframe, status

    def validate_data_product_status(
        self,
        *,
        data_product: OpenDataProductStandard,
        enforce: bool,
        operation: str,
    ) -> None:
        _validate_data_product_status(
            data_product=data_product,
            enforce=enforce,
            operation=operation,
            allowed_statuses=self.allowed_data_product_statuses,
            allow_missing=self.allow_missing_data_product_status,
            case_insensitive=self.data_product_status_case_insensitive,
            failure_message=self.data_product_status_failure_message,
        )

def _check_contract_version(expected: str | None, actual: str) -> None:
    """Check expected contract version constraint against an actual version.

    Supports formats: ``'==x.y.z'``, ``'>=x.y.z'``, or exact string ``'x.y.z'``.
    Raises ``ValueError`` on mismatch.
    """
    if not expected:
        return
    if expected.startswith(">="):
        base = expected[2:]
        if SemVer.parse(actual).major < SemVer.parse(base).major:
            raise ValueError(f"Contract version {actual} does not satisfy {expected}")
    elif expected.startswith("=="):
        if actual != expected[2:]:
            raise ValueError(f"Contract version {actual} != {expected[2:]}")
    else:
        # exact match if plain string
        if actual != expected:
            raise ValueError(f"Contract version {actual} != {expected}")


def _ref_from_contract(contract: OpenDataContractStandard) -> tuple[Optional[str], Optional[str]]:
    """Return ``(path, table)`` derived from the contract's first server.

    The server definition may specify a direct filesystem ``path`` or a logical
    table reference composed from ``catalog``/``schema``/``dataset`` fields.
    """
    if not contract.servers:
        return None, None
    server: Server = contract.servers[0]
    path = server.path
    if path:
        return path, None
    # Build table name from catalog/schema/database/dataset parts when present
    last = server.dataset or server.database
    parts = [server.catalog, server.schema_, last]
    table = ".".join([p for p in parts if p]) if any(parts) else None
    return None, table


def _paths_compatible(provided: str, contract_path: str) -> bool:
    """Return ``True`` when ``provided`` is consistent with ``contract_path``.

    Contracts often describe the root of a dataset (``/data/orders.parquet``)
    while pipelines write versioned outputs beneath it (``/data/orders/1.2.0``).
    This helper treats those layouts as compatible so validation focuses on
    actual mismatches instead of expected directory structures.
    """

    try:
        actual = Path(provided).resolve()
        expected = Path(contract_path).resolve()
    except OSError:
        return False

    if actual == expected:
        return True

    base = expected.parent / expected.stem if expected.suffix else expected
    if actual == base:
        return True

    return base in actual.parents


def _select_version(versions: list[str], minimum: str) -> str:
    """Return the highest version satisfying ``>= minimum``."""

    try:
        base = SemVer.parse(minimum)
    except ValueError as exc:  # pragma: no cover - defensive guard
        raise ValueError(f"Invalid minimum version: {minimum}") from exc

    best: tuple[int, int, int] | None = None
    best_value: Optional[str] = None
    for candidate in versions:
        try:
            parsed = SemVer.parse(candidate)
        except ValueError:
            # Fallback to string comparison when candidate matches exactly.
            if candidate == minimum:
                return candidate
            continue
        key = (parsed.major, parsed.minor, parsed.patch)
        if key < (base.major, base.minor, base.patch):
            continue
        if best is None or key > best:
            best = key
            best_value = candidate
    if best_value is None:
        raise ValueError(f"No versions found satisfying >= {minimum}")
    return best_value


def _resolve_contract(
    *,
    contract_id: str,
    expected_version: Optional[str],
    service: ContractServiceClient | None,
    governance: GovernanceServiceClient | None,
) -> OpenDataContractStandard:
    """Fetch a contract from the configured service respecting version hints."""

    if service is None and governance is None:
        raise ValueError(
            "A contract service or governance service is required when contract_id is provided",
        )

    def _resolve_version(candidate: Optional[str]) -> str:
        if not candidate:
            if governance is not None:
                contract = governance.latest_contract(contract_id=contract_id)
            else:
                contract = service.latest(contract_id)  # type: ignore[union-attr]
            if contract is None:
                raise ValueError(f"No versions available for contract {contract_id}")
            return contract.version

        if candidate.startswith("=="):
            return candidate[2:]

        if candidate.startswith(">="):
            base = candidate[2:]
            versions: Sequence[str]
            if governance is not None:
                versions = tuple(governance.list_contract_versions(contract_id=contract_id))
            else:
                versions = tuple(service.list_versions(contract_id))  # type: ignore[union-attr]
            selected = _select_version(list(versions), base)
            return selected

        return candidate

    version = _resolve_version(expected_version)

    if governance is not None:
        return governance.get_contract(contract_id=contract_id, contract_version=version)

    return service.get(contract_id, version)  # type: ignore[union-attr]


def _enforce_contract_status(
    *,
    handler: object,
    contract: OpenDataContractStandard,
    enforce: bool,
    operation: str,
) -> None:
    """Apply a contract status policy defined by ``handler``."""

    if isinstance(handler, SupportsContractStatusValidation):
        handler.validate_contract_status(
            contract=contract,
            enforce=enforce,
            operation=operation,
        )
        return

    _validate_contract_status(
        contract=contract,
        enforce=enforce,
        operation=operation,
    )


def _validate_contract_status(
    *,
    contract: OpenDataContractStandard,
    enforce: bool,
    operation: str,
    allowed_statuses: Iterable[str] | None = None,
    allow_missing: bool = True,
    case_insensitive: bool = True,
    failure_message: str | None = None,
) -> None:
    """Check the contract status against an allowed set."""

    raw_status = contract.status
    if raw_status is None:
        if allow_missing:
            return
        status_value = ""
    else:
        status_value = str(raw_status).strip()
        if not status_value and allow_missing:
            return

    if not status_value:
        message = (
            failure_message
            or "Contract {contract_id}:{contract_version} status {status!r} "
            "is not allowed for {operation} operations"
        ).format(
            contract_id=str(contract.id or ""),
            contract_version=str(contract.version or ""),
            status=status_value,
            operation=operation,
        )
        if enforce:
            raise ValueError(message)
        logger.warning(message)
        return

    options = allowed_statuses or ("active",)
    allowed = {status.lower() if case_insensitive else status for status in options}
    candidate = status_value.lower() if case_insensitive else status_value
    if candidate in allowed:
        return

    message = (
        failure_message
        or "Contract {contract_id}:{contract_version} status {status!r} "
        "is not allowed for {operation} operations"
    ).format(
        contract_id=str(contract.id or ""),
        contract_version=str(contract.version or ""),
        status=status_value,
        operation=operation,
    )
    if enforce:
        raise ValueError(message)
    logger.warning(message)


def _normalise_version_spec(spec: Optional[str]) -> Optional[str]:
    """Return a normalised version constraint or ``None`` when unset."""

    if spec is None:
        return None
    value = str(spec).strip()
    if not value:
        return None
    if value.startswith("=="):
        return value[2:].strip() or None
    return value


def _check_data_product_version(
    *,
    expected: Optional[str],
    actual: Optional[str],
    data_product_id: str,
    subject: str,
    enforce: bool,
) -> bool:
    """Return ``True`` when ``actual`` satisfies the optional ``expected`` constraint."""

    if expected is None or not expected.strip():
        return True
    if not actual:
        message = (
            f"{subject} version for data product {data_product_id} is unknown; expected {expected}"
        )
        if enforce:
            raise ValueError(message)
        logger.warning(message)
        return False

    requirement = expected.strip()
    if requirement.startswith("=="):
        target = requirement[2:].strip()
        if actual != target:
            message = (
                f"{subject} version {actual} does not satisfy {expected} for data product {data_product_id}"
            )
            if enforce:
                raise ValueError(message)
            logger.warning(message)
            return False
        return True
    if requirement.startswith(">="):
        target = requirement[2:].strip()
        if not target:
            return True
        try:
            if version_key(actual) < version_key(target):
                message = (
                    f"{subject} version {actual} does not satisfy {expected} for data product {data_product_id}"
                )
                if enforce:
                    raise ValueError(message)
                logger.warning(message)
                return False
        except Exception as exc:  # pragma: no cover - defensive against malformed versions
            message = (
                f"Unable to compare versions {actual!r} and {target!r} for data product {data_product_id}: {exc}"
            )
            if enforce:
                raise ValueError(message) from exc
            logger.warning(message)
            return False
        return True
    if actual != requirement:
        message = (
            f"{subject} version {actual} does not satisfy {expected} for data product {data_product_id}"
        )
        if enforce:
            raise ValueError(message)
        logger.warning(message)
        return False
    return True


def _validate_data_product_status(
    *,
    data_product: OpenDataProductStandard,
    enforce: bool,
    operation: str,
    allowed_statuses: Iterable[str] | None = None,
    allow_missing: bool = True,
    case_insensitive: bool = True,
    failure_message: str | None = None,
) -> None:
    """Check the data product status against an allowed set."""

    raw_status = data_product.status
    product_id = str(data_product.id or "")
    product_version = str(data_product.version or "")
    if raw_status is None:
        if allow_missing:
            return
        status_value = ""
    else:
        status_value = str(raw_status).strip()
        if not status_value and allow_missing:
            return

    if not status_value:
        message = (
            failure_message
            or "Data product {data_product_id}@{data_product_version} status {status!r} "
            "is not allowed for {operation} operations"
        ).format(
            data_product_id=product_id,
            data_product_version=product_version,
            status=status_value,
            operation=operation,
        )
        if enforce:
            raise ValueError(message)
        logger.warning(message)
        return

    options = allowed_statuses or ("active",)
    allowed = {status.lower() if case_insensitive else status for status in options}
    candidate = status_value.lower() if case_insensitive else status_value
    if candidate in allowed:
        return

    message = (
        failure_message
        or "Data product {data_product_id}@{data_product_version} status {status!r} "
        "is not allowed for {operation} operations"
    ).format(
        data_product_id=product_id,
        data_product_version=product_version,
        status=status_value,
        operation=operation,
    )
    if enforce:
        raise ValueError(message)
    logger.warning(message)


def _clone_status_handler(handler: object, overrides: Mapping[str, Any]) -> object:
    """Return ``handler`` updated with ``overrides`` without mutating the input."""

    if not overrides:
        return handler
    if is_dataclass(handler):
        try:
            return replace(handler, **overrides)
        except TypeError:
            pass
    try:
        clone = copy.copy(handler)
    except Exception:  # pragma: no cover - fallback when cloning fails
        clone = handler
    for key, value in overrides.items():
        try:
            object.__getattribute__(clone, key)
        except AttributeError:
            continue
        setattr(clone, key, value)
    return clone


def _apply_plan_data_product_policy(
    handler: object,
    plan: Any | None,
    *,
    default_enforce: bool,
) -> tuple[object, bool]:
    """Return a handler/enforcement pair honouring plan-defined policies."""

    if plan is None:
        return handler, default_enforce

    overrides: Dict[str, Any] = {}
    try:
        allowed_statuses = plan.allowed_data_product_statuses  # type: ignore[attr-defined]
    except AttributeError:
        allowed_statuses = None
    if (
        allowed_statuses is not None
        and isinstance(handler, SupportsDataProductStatusPolicy)
    ):
        overrides["allowed_data_product_statuses"] = tuple(allowed_statuses)

    try:
        allow_missing = plan.allow_missing_data_product_status  # type: ignore[attr-defined]
    except AttributeError:
        allow_missing = None
    if (
        allow_missing is not None
        and isinstance(handler, SupportsDataProductStatusPolicy)
    ):
        overrides["allow_missing_data_product_status"] = bool(allow_missing)

    try:
        case_insensitive = plan.data_product_status_case_insensitive  # type: ignore[attr-defined]
    except AttributeError:
        case_insensitive = None
    if (
        case_insensitive is not None
        and isinstance(handler, SupportsDataProductStatusPolicy)
    ):
        overrides["data_product_status_case_insensitive"] = bool(case_insensitive)

    try:
        failure_message = plan.data_product_status_failure_message  # type: ignore[attr-defined]
    except AttributeError:
        failure_message = None
    if (
        failure_message is not None
        and isinstance(handler, SupportsDataProductStatusPolicy)
    ):
        overrides["data_product_status_failure_message"] = failure_message

    handler = _clone_status_handler(handler, overrides)

    try:
        enforce_override = plan.enforce_data_product_status  # type: ignore[attr-defined]
    except AttributeError:
        enforce_override = None
    if enforce_override is None:
        return handler, default_enforce
    return handler, bool(enforce_override)


def _enforce_data_product_status(
    *,
    handler: object,
    data_product: OpenDataProductStandard,
    enforce: bool,
    operation: str,
) -> None:
    """Apply a data product status policy defined by ``handler``."""

    if isinstance(handler, SupportsDataProductStatusValidation):
        handler.validate_data_product_status(
            data_product=data_product,
            enforce=enforce,
            operation=operation,
        )
        return

    _validate_data_product_status(
        data_product=data_product,
        enforce=enforce,
        operation=operation,
    )


def _select_data_product(
    *,
    service: DataProductServiceClient,
    data_product_id: str,
    version_spec: Optional[str],
    handler: object,
    enforce: bool,
    operation: str,
    status_enforce: Optional[bool] = None,
) -> Optional[OpenDataProductStandard]:
    """Return a data product respecting ``handler`` status policies."""

    requirement = version_spec.strip() if isinstance(version_spec, str) else ""
    direct_version = None
    policy_enforce = enforce if status_enforce is None else status_enforce
    if requirement and not requirement.startswith(">="):
        direct_version = _normalise_version_spec(requirement)
    if direct_version:
        try:
            product = service.get(data_product_id, direct_version)
        except Exception:
            if enforce:
                raise
            logger.warning(
                "Data product %s version %s could not be retrieved",
                data_product_id,
                direct_version,
            )
            return None
        _enforce_data_product_status(
            handler=handler,
            data_product=product,
            enforce=policy_enforce,
            operation=operation,
        )
        if not _check_data_product_version(
            expected=requirement,
            actual=product.version,
            data_product_id=data_product_id,
            subject="Data product",
            enforce=enforce,
        ):
            return None
        return product

    latest: Optional[OpenDataProductStandard] = None
    try:
        latest = service.latest(data_product_id)
    except Exception:  # pragma: no cover - defensive logging
        logger.exception("Failed to resolve latest data product %s", data_product_id)

    candidates: list[tuple[Optional[str], Optional[OpenDataProductStandard]]] = []
    if latest is not None:
        candidates.append((latest.version, latest))

    versions: Iterable[str] = ()
    try:
        versions = service.list_versions(data_product_id)
    except Exception:  # pragma: no cover - defensive logging
        logger.exception("Failed to list versions for data product %s", data_product_id)

    seen_versions: set[str] = set()
    if latest and latest.version:
        seen_versions.add(latest.version)

    sorted_versions = sorted(
        (version for version in versions if version),
        key=version_key,
        reverse=True,
    )
    for version in sorted_versions:
        if version in seen_versions:
            continue
        seen_versions.add(version)
        candidates.append((version, None))

    errors: list[str] = []
    for version, product in candidates:
        candidate = product
        if candidate is None and version:
            try:
                candidate = service.get(data_product_id, version)
            except Exception:
                logger.exception(
                    "Failed to load data product %s version %s", data_product_id, version
                )
                continue
        if candidate is None:
            continue
        try:
            _enforce_data_product_status(
                handler=handler,
                data_product=candidate,
                enforce=policy_enforce,
                operation=operation,
            )
        except ValueError as exc:
            errors.append(str(exc))
            continue
        if requirement:
            matches = _check_data_product_version(
                expected=requirement,
                actual=candidate.version,
                data_product_id=data_product_id,
                subject="Data product",
                enforce=enforce,
            )
            if not matches:
                continue
        return candidate

    if errors:
        message = (
            f"Data product {data_product_id} does not have an allowed version for {operation} operations"
        )
        if enforce:
            detail = "; ".join(dict.fromkeys(errors))
            raise ValueError(f"{message}: {detail}")
        logger.warning("%s: %s", message, "; ".join(dict.fromkeys(errors)))
        return None

    if requirement:
        message = (
            f"Data product {data_product_id} has no versions available for {operation} operations"
        )
        if enforce:
            raise ValueError(message)
        logger.warning(message)
    return None


def _load_binding_product_version(
    *,
    service: DataProductServiceClient,
    data_product_id: str,
    version_spec: Optional[str],
    enforce: bool,
    operation: str,
) -> tuple[Optional[OpenDataProductStandard], bool]:
    """Return the exact product version requested by a binding when available."""

    requirement = _normalise_version_spec(version_spec)
    if not requirement or requirement.startswith(">="):
        return None, False
    try:
        product = service.get(data_product_id, requirement)
    except Exception as exc:
        message = (
            f"Data product {data_product_id} version {requirement} is not available "
            f"for {operation} registration"
        )
        if enforce:
            raise ValueError(message) from exc
        logger.warning(message)
        return None, False
    return product, True


class BaseReadExecutor:
    """Shared implementation for batch and streaming read helpers."""

    streaming: bool = False
    require_location: bool = True

    def __init__(
        self,
        *,
        spark: SparkSession,
        contract_id: Optional[str],
        contract_service: Optional[ContractServiceClient],
        expected_contract_version: Optional[str],
        format: Optional[str],
        path: Optional[str],
        table: Optional[str],
        options: Optional[Dict[str, str]],
        enforce: bool,
        auto_cast: bool,
        data_quality_service: Optional[DataQualityServiceClient],
        governance_service: Optional[GovernanceServiceClient],
        data_product_service: Optional[DataProductServiceClient],
        data_product_input: Optional[DataProductInputBinding | Mapping[str, object]],
        dataset_locator: Optional[DatasetLocatorStrategy],
        status_strategy: Optional[ReadStatusStrategy],
        pipeline_context: Optional[PipelineContextLike],
        publication_mode: GovernancePublicationMode | str | None = None,
        plan: Optional[ResolvedReadPlan] = None,
    ) -> None:
        self.spark = spark
        self.plan = plan
        resolved_contract_id = contract_id
        resolved_contract_version = expected_contract_version
        if plan is not None:
            resolved_contract_id = plan.contract_id or resolved_contract_id
            resolved_contract_version = plan.contract_version or resolved_contract_version
        self.contract_id = resolved_contract_id
        self.contract_service = contract_service
        self.expected_contract_version = resolved_contract_version
        self.user_format = format if format is not None else (plan.dataset_format if plan else None)
        self.user_path = path
        self.user_table = table
        self.options = dict(options or {})
        self.enforce = enforce
        self.auto_cast = auto_cast
        self.data_quality_service = data_quality_service
        self.governance_service = governance_service
        self.data_product_service = data_product_service
        if plan is not None and plan.input_binding is not None:
            self.dp_binding = plan.input_binding
        else:
            self.dp_binding = normalise_input_binding(data_product_input)
        self.locator = dataset_locator or ContractFirstDatasetLocator()
        handler = status_strategy or DefaultReadStatusStrategy()
        self.data_product_status_enforce = enforce
        if plan is not None:
            handler, status_enforce = _apply_plan_data_product_policy(
                handler,
                plan,
                default_enforce=enforce,
            )
            self.data_product_status_enforce = status_enforce
        self.status_handler = handler
        if pipeline_context is not None:
            self.pipeline_context = pipeline_context
        elif plan is not None:
            self.pipeline_context = plan.pipeline_context
        else:
            self.pipeline_context = None
        self.publication_mode = self._resolve_publication_mode(
            spark=spark,
            override=publication_mode,
        )
        self.open_data_lineage_only = (
            self.publication_mode is GovernancePublicationMode.OPEN_DATA_LINEAGE
        )
        self.open_telemetry_only = (
            self.publication_mode is GovernancePublicationMode.OPEN_TELEMETRY
        )
        self._skip_governance_activity = self.open_data_lineage_only or self.open_telemetry_only
        self._last_read_resolution: Optional[DatasetResolution] = None

    @staticmethod
    def _resolve_publication_mode(
        *,
        spark: SparkSession,
        override: GovernancePublicationMode | str | None,
    ) -> GovernancePublicationMode:
        config: Dict[str, str] | None = None
        try:
            spark_conf = spark.conf  # type: ignore[attr-defined]
        except Exception:  # pragma: no cover - Spark may be absent in unit tests
            spark_conf = None
        if spark_conf is not None:
            for key in (
                "dc43.governance.publicationMode",
                "dc43.governance.publication_mode",
                "governance.publication.mode",
            ):
                try:
                    value = spark_conf.get(key)
                except Exception:  # pragma: no cover - SparkConf guards may throw
                    value = None
                if value:
                    config = {key: value}
                    break
        return resolve_publication_mode(explicit=override, config=config)

    def execute(self) -> tuple[DataFrame, Optional[ValidationResult]]:
        """Execute the read pipeline and return the dataframe/status pair."""

        contract = self._resolve_contract()
        resolution = self._resolve_resolution(contract)
        dataframe = self._load_dataframe(resolution)
        streaming_active = self._detect_streaming(dataframe)
        dataset_id, dataset_version = self._dataset_identity(resolution, streaming_active)
        (
            dataframe,
            validation,
            expectation_plan,
            contract_identity_tuple,
            assessment,
        ) = self._apply_contract(
            dataframe,
            contract,
            dataset_id,
            dataset_version,
            streaming_active,
            self.pipeline_context,
        )
        status = self._evaluate_governance(
            dataframe,
            contract,
            validation,
            expectation_plan,
            dataset_id,
            dataset_version,
            streaming_active,
            contract_identity_tuple,
            assessment,
        )
        dataframe, status = self.status_handler.apply(
            dataframe=dataframe,
            status=status,
            enforce=self.enforce,
            context=ReadStatusContext(
                contract=contract,
                dataset_id=dataset_id,
                dataset_version=dataset_version,
            ),
        )
        self._register_data_product_input(contract)
        return dataframe, status

    # --- Resolution helpers -------------------------------------------------
    def _resolve_contract(self) -> Optional[OpenDataContractStandard]:
        if self.plan is not None:
            contract = self.plan.contract
            ensure_version(contract)
            _check_contract_version(self.expected_contract_version, contract.version)
            _enforce_contract_status(
                handler=self.status_handler,
                contract=contract,
                enforce=self.enforce,
                operation="read",
            )
            return contract

        contract_id = self.contract_id
        expected_version = self.expected_contract_version
        dp_service = self.data_product_service
        binding = self.dp_binding
        if (
            contract_id is None
            and dp_service is not None
            and binding is not None
            and binding.source_data_product
            and binding.source_output_port
        ):
            product: Optional[OpenDataProductStandard]
            try:
                product = _select_data_product(
                    service=dp_service,
                    data_product_id=binding.source_data_product,
                    version_spec=binding.source_data_product_version,
                    handler=self.status_handler,
                    enforce=self.enforce,
                    operation="read",
                    status_enforce=self.data_product_status_enforce,
                )
            except ValueError:
                if self.enforce:
                    raise
                product = None
            if product is not None:
                port = product.find_output_port(binding.source_output_port)
                if port is None:
                    message = (
                        f"Data product {binding.source_data_product} output port {binding.source_output_port}"
                        " is not defined"
                    )
                    if self.enforce:
                        raise ValueError(message)
                    logger.warning(message)
                else:
                    matches = _check_data_product_version(
                        expected=binding.source_contract_version,
                        actual=port.version,
                        data_product_id=product.id or binding.source_data_product,
                        subject="Output port contract",
                        enforce=self.enforce,
                    )
                    if matches:
                        contract_id = port.contract_id
                        expected_version = (
                            _normalise_version_spec(binding.source_contract_version) or port.version
                        )
                        logger.info(
                            "Resolved contract %s:%s from data product %s output %s",
                            contract_id,
                            expected_version,
                            binding.source_data_product,
                            binding.source_output_port,
                        )

        if (
            contract_id is None
            and dp_service is not None
            and binding is not None
            and binding.source_data_product
            and binding.source_output_port
        ):
            try:
                contract_ref = dp_service.resolve_output_contract(
                    data_product_id=binding.source_data_product,
                    port_name=binding.source_output_port,
                )
            except Exception:  # pragma: no cover - defensive logging
                logger.exception(
                    "Failed to resolve output contract for data product %s port %s",
                    binding.source_data_product,
                    binding.source_output_port,
                )
            else:
                if contract_ref is None:
                    logger.warning(
                        "Data product %s output port %s did not provide a contract reference",
                        binding.source_data_product,
                        binding.source_output_port,
                    )
                else:
                    contract_id, expected_version = contract_ref
                    logger.info(
                        "Resolved contract %s:%s from data product %s output %s",
                        contract_id,
                        expected_version,
                        binding.source_data_product,
                        binding.source_output_port,
                    )

        self.contract_id = contract_id
        self.expected_contract_version = expected_version

        if contract_id is None:
            return None

        contract = _resolve_contract(
            contract_id=contract_id,
            expected_version=expected_version,
            service=self.contract_service,
            governance=_as_governance_service(self.governance_service),
        )
        ensure_version(contract)
        _check_contract_version(expected_version, contract.version)
        _enforce_contract_status(
            handler=self.status_handler,
            contract=contract,
            enforce=self.enforce,
            operation="read",
        )
        return contract

    def _resolve_resolution(
        self, contract: Optional[OpenDataContractStandard]
    ) -> DatasetResolution:
        resolution = self.locator.for_read(
            contract=contract,
            spark=self.spark,
            format=self.user_format,
            path=self.user_path,
            table=self.user_table,
        )

        self._last_read_resolution = resolution

        original_path = self.user_path
        original_table = self.user_table
        original_format = self.user_format

        if contract:
            c_path, c_table = _ref_from_contract(contract)
            c_fmt = contract.servers[0].format if contract.servers else None
            if original_path and c_path and not _paths_compatible(original_path, c_path):
                logger.warning(
                    "Provided path %s does not match contract server path %s",
                    original_path,
                    c_path,
                )
            if original_table and c_table and original_table != c_table:
                logger.warning(
                    "Provided table %s does not match contract server table %s",
                    original_table,
                    c_table,
                )
            if original_format and c_fmt and original_format != c_fmt:
                logger.warning(
                    "Provided format %s does not match contract server format %s",
                    original_format,
                    c_fmt,
                )
            if resolution.format is None:
                resolution.format = c_fmt

        if self.plan is not None and resolution.format is None:
            resolution.format = self.plan.dataset_format

        if (
            self.require_location
            and not resolution.table
            and not (resolution.path or resolution.load_paths)
        ):
            raise ValueError("Either table or path must be provided for read")

        return resolution

    def _load_dataframe(self, resolution: DatasetResolution) -> DataFrame:
        reader = self._build_reader()
        if resolution.format:
            reader = reader.format(resolution.format)

        option_map: Dict[str, str] = {}
        if resolution.read_options:
            option_map.update(resolution.read_options)
        if self.options:
            option_map.update(self.options)
        if option_map:
            reader = reader.options(**option_map)

        target = resolution.load_paths or resolution.path
        if resolution.table:
            return reader.table(resolution.table)
        if target:
            return reader.load(target)
        return reader.load()

    def _build_reader(self):
        return self.spark.readStream if self.streaming else self.spark.read

    def _detect_streaming(self, dataframe: DataFrame) -> bool:
        try:
            is_streaming = bool(dataframe.isStreaming)  # type: ignore[attr-defined]
        except AttributeError:
            is_streaming = False
        streaming_active = self.streaming or is_streaming
        if streaming_active and not self.streaming:
            logger.info("Detected streaming dataframe; enabling streaming mode")
        return streaming_active

    def _dataset_identity(
        self,
        resolution: DatasetResolution,
        streaming_active: bool,
    ) -> tuple[str, str]:
        dataset_id = (
            (self.plan.dataset_id if self.plan and self.plan.dataset_id else None)
            or resolution.dataset_id
            or dataset_id_from_ref(
                table=resolution.table,
                path=resolution.path,
            )
        )
        observed_version = (
            (self.plan.dataset_version if self.plan and self.plan.dataset_version else None)
            or resolution.dataset_version
            or get_delta_version(
                self.spark,
                table=resolution.table,
                path=resolution.path,
            )
        )
        dataset_version = observed_version or self._default_dataset_version(streaming_active)
        return dataset_id, dataset_version

    def _default_dataset_version(self, streaming_active: bool) -> str:
        return "unknown"

    def _should_collect_metrics(self, streaming_active: bool) -> bool:
        return not streaming_active

    def _normalise_streaming_validation(
        self, validation: ValidationResult, *, streaming_active: bool
    ) -> None:
        if not streaming_active:
            return
        warnings = list(validation.warnings or [])
        if not warnings:
            validation.merge_details({"streaming_metrics_deferred": True})
            return
        filtered = [
            warning
            for warning in warnings
            if "violation counts were not provided" not in warning
            and not warning.startswith("missing metric for expectation")
        ]
        if len(filtered) != len(warnings):
            validation.warnings = filtered
            if not filtered:
                validation.reason = None
        validation.merge_details({"streaming_metrics_deferred": True})

    def _apply_contract(
        self,
        dataframe: DataFrame,
        contract: Optional[OpenDataContractStandard],
        dataset_id: str,
        dataset_version: str,
        streaming_active: bool,
        pipeline_context: Optional[PipelineContextLike],
    ) -> tuple[
        DataFrame,
        Optional[ValidationResult],
        list[Mapping[str, Any]],
        Optional[tuple[str, str]],
        Optional[QualityAssessment],
    ]:
        if contract is None:
            return dataframe, None, [], None, None

        dq_service = self.data_quality_service
        governance_client = _as_governance_service(self.governance_service)
        if dq_service is None and governance_client is None:
            raise ValueError(
                "data_quality_service or governance_service is required when validating against a contract",
            )

        cid, cver = contract_identity(contract)
        logger.info("Reading with contract %s:%s", cid, cver)
        expectation_plan: list[Mapping[str, Any]] = []

        assessment: Optional[QualityAssessment] = None
        validation: Optional[ValidationResult]
        if dq_service is not None:
            expectation_plan = list(dq_service.describe_expectations(contract=contract))
        elif governance_client is not None:
            expectation_plan = list(
                governance_client.describe_expectations(
                    contract_id=cid,
                    contract_version=cver,
                )
            )

        observed_schema, observed_metrics = collect_observations(
            dataframe,
            contract,
            expectations=expectation_plan,
            collect_metrics=self._should_collect_metrics(streaming_active),
        )
        if streaming_active and observed_metrics == {}:
            logger.info(
                "Streaming read for %s:%s validated without collecting Spark metrics",
                cid,
                cver,
            )

        if dq_service is not None:
            validation = _evaluate_with_service(
                contract=contract,
                service=dq_service,
                schema=observed_schema,
                metrics=observed_metrics,
            )
        else:
            base_pipeline_context = normalise_pipeline_context(pipeline_context)

            def _observations() -> ObservationPayload:
                return ObservationPayload(
                    metrics=dict(observed_metrics or {}),
                    schema=dict(observed_schema or {}),
                    reused=True,
                )

            assessment = governance_client.evaluate_dataset(
                contract_id=cid,
                contract_version=cver,
                dataset_id=dataset_id,
                dataset_version=dataset_version,
                validation=None,
                observations=_observations,
                pipeline_context=base_pipeline_context,
                operation="read",
                bump=self.plan.bump if self.plan else "minor",
                draft_on_violation=self.plan.draft_on_violation if self.plan else False,
            )
            validation = assessment.validation or assessment.status
            if validation is None:
                validation = ValidationResult(
                    ok=True, errors=[], warnings=[], metrics={}, schema={}
                )

            self._normalise_streaming_validation(validation, streaming_active=streaming_active)
            if expectation_plan and "expectation_plan" not in validation.details:
                validation.merge_details({"expectation_plan": expectation_plan})
        if "dataset_version" not in validation.details or validation.details.get("dataset_id") is None:
            validation.merge_details(
                {
                    "dataset_id": dataset_id,
                    "dataset_version": dataset_version,
                }
            )
        _annotate_observation_scope(
            validation,
            operation="read",
            scope="input_slice",
        )
        logger.info(
            "Read validation: ok=%s errors=%s warnings=%s",
            validation.ok,
            validation.errors,
            validation.warnings,
        )
        if not validation.ok and self.enforce:
            raise ValueError(f"Contract validation failed: {validation.errors}")

        dataframe = apply_contract(dataframe, contract, auto_cast=self.auto_cast)
        return dataframe, validation, expectation_plan, (cid, cver), assessment

    def _evaluate_governance(
        self,
        dataframe: DataFrame,
        contract: Optional[OpenDataContractStandard],
        validation: Optional[ValidationResult],
        expectation_plan: list[Mapping[str, Any]],
        dataset_id: str,
        dataset_version: str,
        streaming_active: bool,
        contract_identity_tuple: Optional[tuple[str, str]],
        assessment: Optional[QualityAssessment],
    ) -> Optional[ValidationResult]:
        governance_client = _as_governance_service(self.governance_service)
        if governance_client is None or contract is None or contract_identity_tuple is None:
            return None

        cid, cver = contract_identity_tuple
        base_pipeline_context = normalise_pipeline_context(self.pipeline_context)

        if assessment is None:
            if validation is None:
                return None

            def _observations() -> ObservationPayload:
                metrics_payload, schema_payload, reused = build_metrics_payload(
                    dataframe,
                    contract,
                    validation=validation,
                    include_schema=True,
                    expectations=expectation_plan,
                    collect_metrics=self._should_collect_metrics(streaming_active),
                )
                if reused:
                    logger.info(
                        "Using cached validation metrics for %s@%s", dataset_id, dataset_version
                    )
                elif streaming_active:
                    logger.info(
                        "Streaming read for %s@%s defers Spark metric collection",
                        dataset_id,
                        dataset_version,
                    )
                else:
                    logger.info("Computing DQ metrics for %s@%s", dataset_id, dataset_version)
                return ObservationPayload(
                    metrics=metrics_payload,
                    schema=schema_payload,
                    reused=reused,
                )

            assessment = governance_client.evaluate_dataset(
                contract_id=cid,
                contract_version=cver,
                dataset_id=dataset_id,
                dataset_version=dataset_version,
                validation=validation,
                observations=_observations,
                pipeline_context=base_pipeline_context,
                operation="read",
                bump=self.plan.bump if self.plan else "minor",
                draft_on_violation=self.plan.draft_on_violation if self.plan else False,
            )

        status = assessment.status
        if status is None and assessment.validation is not None:
            status = assessment.validation
        if self.plan is not None and not self._skip_governance_activity:
            try:
                governance_client.register_read_activity(
                    plan=self.plan,
                    assessment=assessment,
                )
            except RuntimeError:
                raise
            except ValueError as exc:
                if self.enforce:
                    raise
                logger.warning("Governance read activity rejected: %s", exc)
            except Exception:  # pragma: no cover - defensive logging
                logger.exception(
                    "Failed to register governance read activity for %s",
                    self.plan.contract_id,
                )
        if self.open_data_lineage_only and governance_client is not None:
            try:
                resolution = self._last_read_resolution
                dataset_format = None
                dataset_path = None
                dataset_table = None
                if resolution is not None:
                    dataset_format = resolution.format
                    dataset_table = resolution.table
                    dataset_path = resolution.path
                    if dataset_path is None and resolution.load_paths:
                        dataset_path = resolution.load_paths[0]
                lineage_event = build_lineage_run_event(
                    operation="read",
                    plan=self.plan,
                    pipeline_context=self.pipeline_context,
                    contract_id=cid,
                    contract_version=cver,
                    dataset_id=dataset_id,
                    dataset_version=dataset_version,
                    dataset_format=dataset_format or self.user_format,
                    table=dataset_table or self.user_table,
                    path=dataset_path or self.user_path,
                    binding=self.dp_binding,
                    validation=validation,
                    status=status,
                    expectation_plan=expectation_plan,
                )
                governance_client.publish_lineage_event(event=lineage_event)
            except Exception:  # pragma: no cover - defensive logging
                logger.exception(
                    "Failed to publish lineage run for %s:%s",
                    cid,
                    cver,
                )
        if self.open_telemetry_only:
            try:
                resolution = self._last_read_resolution
                dataset_format = None
                dataset_path = None
                dataset_table = None
                if resolution is not None:
                    dataset_format = resolution.format
                    dataset_table = resolution.table
                    dataset_path = resolution.path
                    if dataset_path is None and resolution.load_paths:
                        dataset_path = resolution.load_paths[0]
                record_telemetry_span(
                    operation="read",
                    plan=self.plan,
                    pipeline_context=self.pipeline_context,
                    contract_id=cid,
                    contract_version=cver,
                    dataset_id=dataset_id,
                    dataset_version=dataset_version,
                    dataset_format=dataset_format or self.user_format,
                    table=dataset_table or self.user_table,
                    path=dataset_path or self.user_path,
                    binding=self.dp_binding,
                    validation=validation,
                    status=status,
                    expectation_plan=expectation_plan,
                )
            except Exception:  # pragma: no cover - defensive logging
                logger.exception(
                    "Failed to record telemetry span for %s:%s",
                    cid,
                    dataset_id,
                )
        if status:
            logger.info("DQ status for %s@%s: %s", dataset_id, dataset_version, status.status)
            status.merge_details({
                "dataset_id": dataset_id,
                "dataset_version": dataset_version,
            })
            _annotate_observation_scope(
                status,
                operation="read",
                scope="input_slice",
            )
        return status

    def _register_data_product_input(
        self, contract: Optional[OpenDataContractStandard]
    ) -> None:
        dp_service = self.data_product_service
        binding = self.dp_binding
        if dp_service is None or binding is None or contract is None:
            return
        if not binding.data_product:
            logger.warning(
                "data_product_input requires a data_product identifier to register input ports",
            )
            return

        port_name = binding.port_name or binding.source_output_port or contract.id
        try:
            registration = dp_service.register_input_port(
                data_product_id=binding.data_product,
                port_name=port_name,
                contract_id=contract.id,
                contract_version=contract.version,
                bump=binding.bump,
                custom_properties=binding.custom_properties,
                source_data_product=binding.source_data_product,
                source_output_port=binding.source_output_port,
            )
        except Exception:  # pragma: no cover - defensive logging
            logger.exception(
                "Failed to register data product input port %s on %s",
                port_name,
                binding.data_product,
            )
            return

        if registration.changed:
            product = registration.product
            version = product.version or "<unknown>"
            if (product.status or "").lower() != "draft":
                raise RuntimeError(
                    "Data product input registration did not produce a draft version"
                )
            raise RuntimeError(
                "Data product %s input port %s requires review at version %s",
                binding.data_product,
                port_name,
                version,
            )

        product = registration.product
        requested_version = binding.data_product_version
        matched_spec = False
        if requested_version:
            resolved_product, matched_spec = _load_binding_product_version(
                service=dp_service,
                data_product_id=binding.data_product,
                version_spec=requested_version,
                enforce=self.enforce,
                operation="input",
            )
            if resolved_product is not None:
                product = resolved_product
        _enforce_data_product_status(
            handler=self.status_handler,
            data_product=product,
            enforce=self.data_product_status_enforce,
            operation="read",
        )
        if binding.data_product_version and matched_spec:
            _check_data_product_version(
                expected=binding.data_product_version,
                actual=product.version,
                data_product_id=binding.data_product,
                subject="Data product",
                enforce=self.enforce,
            )


class BatchReadExecutor(BaseReadExecutor):
    """Batch-only read execution."""


class StreamingReadExecutor(BaseReadExecutor):
    """Streaming read execution with dataset version fallbacks."""

    streaming = True
    require_location = False

    def _default_dataset_version(self, streaming_active: bool) -> str:  # noqa: D401
        if streaming_active:
            return _timestamp()
        return super()._default_dataset_version(streaming_active)


def _execute_read(
    executor_cls: Type[BaseReadExecutor],
    *,
    spark: SparkSession,
    contract_id: Optional[str],
    contract_service: Optional[ContractServiceClient],
    expected_contract_version: Optional[str],
    format: Optional[str],
    path: Optional[str],
    table: Optional[str],
    options: Optional[Dict[str, str]],
    enforce: bool,
    auto_cast: bool,
    data_quality_service: Optional[DataQualityServiceClient],
    governance_service: Optional[GovernanceServiceClient],
    data_product_service: Optional[DataProductServiceClient],
    data_product_input: Optional[DataProductInputBinding | Mapping[str, object]],
    dataset_locator: Optional[DatasetLocatorStrategy],
    status_strategy: Optional[ReadStatusStrategy],
    pipeline_context: Optional[PipelineContextLike],
    publication_mode: GovernancePublicationMode | str | None,
    return_status: bool,
    plan: Optional[ResolvedReadPlan] = None,
) -> DataFrame | tuple[DataFrame, Optional[ValidationResult]]:
    executor = executor_cls(
        spark=spark,
        contract_id=contract_id,
        contract_service=contract_service,
        expected_contract_version=expected_contract_version,
        format=format,
        path=path,
        table=table,
        options=options,
        enforce=enforce,
        auto_cast=auto_cast,
        data_quality_service=data_quality_service,
        governance_service=governance_service,
        data_product_service=data_product_service,
        data_product_input=data_product_input,
        dataset_locator=dataset_locator,
        status_strategy=status_strategy,
        pipeline_context=pipeline_context,
        publication_mode=publication_mode,
        plan=plan,
    )
    dataframe, status = executor.execute()
    return (dataframe, status) if return_status else dataframe


# Overloads help type checkers infer tuple returns when ``return_status`` is True.
@overload
def read_with_contract(
    spark: SparkSession,
    *,
    contract_id: Optional[str] = None,
    contract_service: Optional[ContractServiceClient] = None,
    expected_contract_version: Optional[str] = None,
    format: Optional[str] = None,
    path: Optional[str] = None,
    table: Optional[str] = None,
    options: Optional[Dict[str, str]] = None,
    enforce: bool = True,
    auto_cast: bool = True,
    data_quality_service: Optional[DataQualityServiceClient] = None,
    governance_service: Optional[GovernanceServiceClient] = None,
    data_product_service: Optional[DataProductServiceClient] = None,
    data_product_input: Optional[DataProductInputBinding | Mapping[str, object]] = None,
    dataset_locator: Optional[DatasetLocatorStrategy] = None,
    status_strategy: Optional[ReadStatusStrategy] = None,
    pipeline_context: Optional[PipelineContextLike] = None,
    return_status: Literal[True] = True,
) -> tuple[DataFrame, Optional[ValidationResult]]:
    ...


@overload
def read_with_contract(
    spark: SparkSession,
    *,
    contract_id: Optional[str] = None,
    contract_service: Optional[ContractServiceClient] = None,
    expected_contract_version: Optional[str] = None,
    format: Optional[str] = None,
    path: Optional[str] = None,
    table: Optional[str] = None,
    options: Optional[Dict[str, str]] = None,
    enforce: bool = True,
    auto_cast: bool = True,
    data_quality_service: Optional[DataQualityServiceClient] = None,
    governance_service: Optional[GovernanceServiceClient] = None,
    data_product_service: Optional[DataProductServiceClient] = None,
    data_product_input: Optional[DataProductInputBinding | Mapping[str, object]] = None,
    dataset_locator: Optional[DatasetLocatorStrategy] = None,
    status_strategy: Optional[ReadStatusStrategy] = None,
    pipeline_context: Optional[PipelineContextLike] = None,
    return_status: Literal[False],
) -> DataFrame:
    ...


@overload
def read_with_contract(
    spark: SparkSession,
    *,
    contract_id: Optional[str] = None,
    contract_service: Optional[ContractServiceClient] = None,
    expected_contract_version: Optional[str] = None,
    format: Optional[str] = None,
    path: Optional[str] = None,
    table: Optional[str] = None,
    options: Optional[Dict[str, str]] = None,
    enforce: bool = True,
    auto_cast: bool = True,
    data_quality_service: Optional[DataQualityServiceClient] = None,
    governance_service: Optional[GovernanceServiceClient] = None,
    data_product_service: Optional[DataProductServiceClient] = None,
    data_product_input: Optional[DataProductInputBinding | Mapping[str, object]] = None,
    dataset_locator: Optional[DatasetLocatorStrategy] = None,
    status_strategy: Optional[ReadStatusStrategy] = None,
    pipeline_context: Optional[PipelineContextLike] = None,
    return_status: bool = True,
) -> DataFrame | tuple[DataFrame, Optional[ValidationResult]]:
    ...


def read_with_contract(
    spark: SparkSession,
    *,
    contract_id: Optional[str] = None,
    contract_service: Optional[ContractServiceClient] = None,
    expected_contract_version: Optional[str] = None,
    format: Optional[str] = None,
    path: Optional[str] = None,
    table: Optional[str] = None,
    options: Optional[Dict[str, str]] = None,
    enforce: bool = True,
    auto_cast: bool = True,
    data_quality_service: Optional[DataQualityServiceClient] = None,
    governance_service: Optional[GovernanceServiceClient] = None,
    data_product_service: Optional[DataProductServiceClient] = None,
    data_product_input: Optional[DataProductInputBinding | Mapping[str, object]] = None,
    dataset_locator: Optional[DatasetLocatorStrategy] = None,
    status_strategy: Optional[ReadStatusStrategy] = None,
    pipeline_context: Optional[PipelineContextLike] = None,
    return_status: bool = True,
) -> DataFrame | tuple[DataFrame, Optional[ValidationResult]]:
    """Read a batch DataFrame with contract enforcement and governance hooks.

    .. deprecated:: 0.0
       Use :func:`read_with_governance` with a :class:`GovernanceSparkReadRequest`.
    """

    _warn_deprecated("read_with_contract", "read_with_governance")

    return _execute_read(
        BatchReadExecutor,
        spark=spark,
        contract_id=contract_id,
        contract_service=contract_service,
        expected_contract_version=expected_contract_version,
        format=format,
        path=path,
        table=table,
        options=options,
        enforce=enforce,
        auto_cast=auto_cast,
        data_quality_service=data_quality_service,
        governance_service=governance_service,
        data_product_service=data_product_service,
        data_product_input=data_product_input,
        dataset_locator=dataset_locator,
        status_strategy=status_strategy,
        pipeline_context=pipeline_context,
        publication_mode=None,
        return_status=return_status,
    )


@overload
def read_with_governance(
    spark: SparkSession,
    request: GovernanceSparkReadRequest | Mapping[str, object],
    *,
    governance_service: GovernanceServiceClient,
    enforce: bool = True,
    auto_cast: bool = True,
    return_status: Literal[True] = True,
) -> tuple[DataFrame, Optional[ValidationResult]]:
    ...


@overload
def read_with_governance(
    spark: SparkSession,
    request: GovernanceSparkReadRequest | Mapping[str, object],
    *,
    governance_service: GovernanceServiceClient,
    enforce: bool = True,
    auto_cast: bool = True,
    return_status: Literal[False],
) -> DataFrame:
    ...


@overload
def read_with_governance(
    spark: SparkSession,
    request: GovernanceSparkReadRequest | Mapping[str, object],
    *,
    governance_service: GovernanceServiceClient,
    enforce: bool = True,
    auto_cast: bool = True,
    return_status: bool = True,
) -> DataFrame | tuple[DataFrame, Optional[ValidationResult]]:
    ...


def read_with_governance(
    spark: SparkSession,
    request: GovernanceSparkReadRequest | Mapping[str, object],
    *,
    governance_service: GovernanceServiceClient,
    enforce: bool = True,
    auto_cast: bool = True,
    return_status: bool = True,
) -> DataFrame | tuple[DataFrame, Optional[ValidationResult]]:
    """Read a DataFrame relying solely on governance context resolution."""

    if not isinstance(request, GovernanceSparkReadRequest):
        if isinstance(request, Mapping):
            request = GovernanceSparkReadRequest(**dict(request))
        else:
            raise TypeError("request must be a GovernanceSparkReadRequest or mapping")

    strategy = request.status_strategy or DefaultReadStatusStrategy()
    context = request.context
    if (
        context.allowed_data_product_statuses is None
        and isinstance(strategy, SupportsDataProductStatusPolicy)
    ):
        allowed = strategy.allowed_data_product_statuses
        if isinstance(allowed, str):
            context.allowed_data_product_statuses = (allowed,)
        else:
            context.allowed_data_product_statuses = tuple(allowed)
    if (
        context.allow_missing_data_product_status is None
        and isinstance(strategy, SupportsDataProductStatusPolicy)
    ):
        context.allow_missing_data_product_status = bool(
            strategy.allow_missing_data_product_status
        )
    if (
        context.data_product_status_case_insensitive is None
        and isinstance(strategy, SupportsDataProductStatusPolicy)
    ):
        context.data_product_status_case_insensitive = bool(
            strategy.data_product_status_case_insensitive
        )
    if (
        context.data_product_status_failure_message is None
        and isinstance(strategy, SupportsDataProductStatusPolicy)
    ):
        failure_message = strategy.data_product_status_failure_message
        if failure_message is not None:
            context.data_product_status_failure_message = str(failure_message)
    if context.enforce_data_product_status is None:
        context.enforce_data_product_status = bool(enforce)

    plan = governance_service.resolve_read_context(context=context)
    pipeline_ctx = request.context.pipeline_context or plan.pipeline_context

    return _execute_read(
        BatchReadExecutor,
        spark=spark,
        contract_id=plan.contract_id,
        contract_service=None,
        expected_contract_version=plan.contract_version,
        format=request.format,
        path=request.path,
        table=request.table,
        options=request.options,
        enforce=enforce,
        auto_cast=auto_cast,
        data_quality_service=None,
        governance_service=governance_service,
        data_product_service=None,
        data_product_input=None,
        dataset_locator=request.dataset_locator,
        status_strategy=strategy,
        pipeline_context=pipeline_ctx,
        publication_mode=request.publication_mode,
        return_status=return_status,
        plan=plan,
    )


@overload
def read_stream_with_contract(
    *,
    spark: SparkSession,
    contract_id: Optional[str] = None,
    contract_service: Optional[ContractServiceClient] = None,
    expected_contract_version: Optional[str] = None,
    format: Optional[str] = None,
    path: Optional[str] = None,
    table: Optional[str] = None,
    options: Optional[Dict[str, str]] = None,
    enforce: bool = True,
    auto_cast: bool = True,
    data_quality_service: Optional[DataQualityServiceClient] = None,
    governance_service: Optional[GovernanceServiceClient] = None,
    data_product_service: Optional[DataProductServiceClient] = None,
    data_product_input: Optional[DataProductInputBinding | Mapping[str, object]] = None,
    dataset_locator: Optional[DatasetLocatorStrategy] = None,
    status_strategy: Optional[ReadStatusStrategy] = None,
    pipeline_context: Optional[PipelineContextLike] = None,
    return_status: Literal[True] = True,
) -> tuple[DataFrame, Optional[ValidationResult]]:
    ...


@overload
def read_stream_with_contract(
    *,
    spark: SparkSession,
    contract_id: Optional[str] = None,
    contract_service: Optional[ContractServiceClient] = None,
    expected_contract_version: Optional[str] = None,
    format: Optional[str] = None,
    path: Optional[str] = None,
    table: Optional[str] = None,
    options: Optional[Dict[str, str]] = None,
    enforce: bool = True,
    auto_cast: bool = True,
    data_quality_service: Optional[DataQualityServiceClient] = None,
    governance_service: Optional[GovernanceServiceClient] = None,
    data_product_service: Optional[DataProductServiceClient] = None,
    data_product_input: Optional[DataProductInputBinding | Mapping[str, object]] = None,
    dataset_locator: Optional[DatasetLocatorStrategy] = None,
    status_strategy: Optional[ReadStatusStrategy] = None,
    pipeline_context: Optional[PipelineContextLike] = None,
    return_status: Literal[False],
) -> DataFrame:
    ...


@overload
def read_stream_with_contract(
    *,
    spark: SparkSession,
    contract_id: Optional[str] = None,
    contract_service: Optional[ContractServiceClient] = None,
    expected_contract_version: Optional[str] = None,
    format: Optional[str] = None,
    path: Optional[str] = None,
    table: Optional[str] = None,
    options: Optional[Dict[str, str]] = None,
    enforce: bool = True,
    auto_cast: bool = True,
    data_quality_service: Optional[DataQualityServiceClient] = None,
    governance_service: Optional[GovernanceServiceClient] = None,
    data_product_service: Optional[DataProductServiceClient] = None,
    data_product_input: Optional[DataProductInputBinding | Mapping[str, object]] = None,
    dataset_locator: Optional[DatasetLocatorStrategy] = None,
    status_strategy: Optional[ReadStatusStrategy] = None,
    pipeline_context: Optional[PipelineContextLike] = None,
    return_status: bool = True,
) -> DataFrame | tuple[DataFrame, Optional[ValidationResult]]:
    ...


def read_stream_with_contract(
    *,
    spark: SparkSession,
    contract_id: Optional[str] = None,
    contract_service: Optional[ContractServiceClient] = None,
    expected_contract_version: Optional[str] = None,
    format: Optional[str] = None,
    path: Optional[str] = None,
    table: Optional[str] = None,
    options: Optional[Dict[str, str]] = None,
    enforce: bool = True,
    auto_cast: bool = True,
    data_quality_service: Optional[DataQualityServiceClient] = None,
    governance_service: Optional[GovernanceServiceClient] = None,
    data_product_service: Optional[DataProductServiceClient] = None,
    data_product_input: Optional[DataProductInputBinding | Mapping[str, object]] = None,
    dataset_locator: Optional[DatasetLocatorStrategy] = None,
    status_strategy: Optional[ReadStatusStrategy] = None,
    pipeline_context: Optional[PipelineContextLike] = None,
    return_status: bool = True,
) -> DataFrame | tuple[DataFrame, Optional[ValidationResult]]:
    """Create a streaming ``DataFrame`` while enforcing an ODCS contract.

    .. deprecated:: 0.0
       Use :func:`read_stream_with_governance` with a :class:`GovernanceSparkReadRequest`.
    """

    _warn_deprecated("read_stream_with_contract", "read_stream_with_governance")

    return _execute_read(
        StreamingReadExecutor,
        spark=spark,
        contract_id=contract_id,
        contract_service=contract_service,
        expected_contract_version=expected_contract_version,
        format=format,
        path=path,
        table=table,
        options=options,
        enforce=enforce,
        auto_cast=auto_cast,
        data_quality_service=data_quality_service,
        governance_service=governance_service,
        data_product_service=data_product_service,
        data_product_input=data_product_input,
        dataset_locator=dataset_locator,
        status_strategy=status_strategy,
        pipeline_context=pipeline_context,
        publication_mode=None,
        return_status=return_status,
    )


@overload
def read_stream_with_governance(
    spark: SparkSession,
    request: GovernanceSparkReadRequest | Mapping[str, object],
    *,
    governance_service: GovernanceServiceClient,
    enforce: bool = True,
    auto_cast: bool = True,
    return_status: Literal[True] = True,
) -> tuple[DataFrame, Optional[ValidationResult]]:
    ...


@overload
def read_stream_with_governance(
    spark: SparkSession,
    request: GovernanceSparkReadRequest | Mapping[str, object],
    *,
    governance_service: GovernanceServiceClient,
    enforce: bool = True,
    auto_cast: bool = True,
    return_status: Literal[False],
) -> DataFrame:
    ...


@overload
def read_stream_with_governance(
    spark: SparkSession,
    request: GovernanceSparkReadRequest | Mapping[str, object],
    *,
    governance_service: GovernanceServiceClient,
    enforce: bool = True,
    auto_cast: bool = True,
    return_status: bool = True,
) -> DataFrame | tuple[DataFrame, Optional[ValidationResult]]:
    ...


def read_stream_with_governance(
    spark: SparkSession,
    request: GovernanceSparkReadRequest | Mapping[str, object],
    *,
    governance_service: GovernanceServiceClient,
    enforce: bool = True,
    auto_cast: bool = True,
    return_status: bool = True,
) -> DataFrame | tuple[DataFrame, Optional[ValidationResult]]:
    """Streaming variant of :func:`read_with_governance`."""

    if not isinstance(request, GovernanceSparkReadRequest):
        if isinstance(request, Mapping):
            request = GovernanceSparkReadRequest(**dict(request))
        else:
            raise TypeError("request must be a GovernanceSparkReadRequest or mapping")

    plan = governance_service.resolve_read_context(context=request.context)
    pipeline_ctx = request.context.pipeline_context or plan.pipeline_context

    return _execute_read(
        StreamingReadExecutor,
        spark=spark,
        contract_id=plan.contract_id,
        contract_service=None,
        expected_contract_version=plan.contract_version,
        format=request.format,
        path=request.path,
        table=request.table,
        options=request.options,
        enforce=enforce,
        auto_cast=auto_cast,
        data_quality_service=None,
        governance_service=governance_service,
        data_product_service=None,
        data_product_input=None,
        dataset_locator=request.dataset_locator,
        status_strategy=request.status_strategy,
        pipeline_context=pipeline_ctx,
        publication_mode=request.publication_mode,
        return_status=return_status,
        plan=plan,
    )

def read_from_contract(
    spark: SparkSession,
    *,
    contract_id: str,
    contract_service: ContractServiceClient,
    expected_contract_version: Optional[str] = None,
    format: Optional[str] = None,
    path: Optional[str] = None,
    table: Optional[str] = None,
    options: Optional[Dict[str, str]] = None,
    enforce: bool = True,
    auto_cast: bool = True,
    data_quality_service: Optional[DataQualityServiceClient] = None,
    governance_service: Optional[GovernanceServiceClient] = None,
    dataset_locator: Optional[DatasetLocatorStrategy] = None,
    status_strategy: Optional[ReadStatusStrategy] = None,
    pipeline_context: Optional[PipelineContextLike] = None,
    return_status: bool = True,
) -> DataFrame | Tuple[DataFrame, Optional[ValidationResult]]:
    """Read and validate a dataset by referencing a contract identifier directly.

    .. deprecated:: 0.0
       Use :func:`read_with_governance`.
    """

    _warn_deprecated("read_from_contract", "read_with_governance")

    return read_with_contract(
        spark,
        contract_id=contract_id,
        contract_service=contract_service,
        expected_contract_version=expected_contract_version,
        format=format,
        path=path,
        table=table,
        options=options,
        enforce=enforce,
        auto_cast=auto_cast,
        data_quality_service=data_quality_service,
        governance_service=governance_service,
        dataset_locator=dataset_locator,
        status_strategy=status_strategy,
        pipeline_context=pipeline_context,
        return_status=return_status,
    )


def read_stream_from_contract(
    *,
    spark: SparkSession,
    contract_id: str,
    contract_service: ContractServiceClient,
    expected_contract_version: Optional[str] = None,
    format: Optional[str] = None,
    path: Optional[str] = None,
    table: Optional[str] = None,
    options: Optional[Dict[str, str]] = None,
    enforce: bool = True,
    auto_cast: bool = True,
    data_quality_service: Optional[DataQualityServiceClient] = None,
    governance_service: Optional[GovernanceServiceClient] = None,
    dataset_locator: Optional[DatasetLocatorStrategy] = None,
    status_strategy: Optional[ReadStatusStrategy] = None,
    pipeline_context: Optional[PipelineContextLike] = None,
    return_status: bool = True,
) -> DataFrame | Tuple[DataFrame, Optional[ValidationResult]]:
    """Streaming counterpart to :func:`read_from_contract`.

    .. deprecated:: 0.0
       Use :func:`read_stream_with_governance`.
    """

    _warn_deprecated("read_stream_from_contract", "read_stream_with_governance")

    return read_stream_with_contract(
        spark=spark,
        contract_id=contract_id,
        contract_service=contract_service,
        expected_contract_version=expected_contract_version,
        format=format,
        path=path,
        table=table,
        options=options,
        enforce=enforce,
        auto_cast=auto_cast,
        data_quality_service=data_quality_service,
        governance_service=governance_service,
        dataset_locator=dataset_locator,
        status_strategy=status_strategy,
        pipeline_context=pipeline_context,
        return_status=return_status,
    )


def read_from_data_product(
    spark: SparkSession,
    *,
    data_product_service: DataProductServiceClient,
    data_product_input: DataProductInputBinding | Mapping[str, object],
    expected_contract_version: Optional[str] = None,
    contract_service: Optional[ContractServiceClient] = None,
    format: Optional[str] = None,
    path: Optional[str] = None,
    table: Optional[str] = None,
    options: Optional[Dict[str, str]] = None,
    enforce: bool = True,
    auto_cast: bool = True,
    data_quality_service: Optional[DataQualityServiceClient] = None,
    governance_service: Optional[GovernanceServiceClient] = None,
    dataset_locator: Optional[DatasetLocatorStrategy] = None,
    status_strategy: Optional[ReadStatusStrategy] = None,
    pipeline_context: Optional[PipelineContextLike] = None,
    return_status: bool = True,
) -> DataFrame | Tuple[DataFrame, Optional[ValidationResult]]:
    """Read a dataset by resolving the contract from a data product input binding.

    .. deprecated:: 0.0
       Use :func:`read_with_governance` and resolve bindings through governance.
    """

    _warn_deprecated("read_from_data_product", "read_with_governance")

    return read_with_contract(
        spark,
        contract_service=contract_service,
        expected_contract_version=expected_contract_version,
        data_product_service=data_product_service,
        data_product_input=data_product_input,
        format=format,
        path=path,
        table=table,
        options=options,
        enforce=enforce,
        auto_cast=auto_cast,
        data_quality_service=data_quality_service,
        governance_service=governance_service,
        dataset_locator=dataset_locator,
        status_strategy=status_strategy,
        pipeline_context=pipeline_context,
        return_status=return_status,
    )


def read_stream_from_data_product(
    *,
    spark: SparkSession,
    data_product_service: DataProductServiceClient,
    data_product_input: DataProductInputBinding | Mapping[str, object],
    expected_contract_version: Optional[str] = None,
    contract_service: Optional[ContractServiceClient] = None,
    format: Optional[str] = None,
    path: Optional[str] = None,
    table: Optional[str] = None,
    options: Optional[Dict[str, str]] = None,
    enforce: bool = True,
    auto_cast: bool = True,
    data_quality_service: Optional[DataQualityServiceClient] = None,
    governance_service: Optional[GovernanceServiceClient] = None,
    dataset_locator: Optional[DatasetLocatorStrategy] = None,
    status_strategy: Optional[ReadStatusStrategy] = None,
    pipeline_context: Optional[PipelineContextLike] = None,
    return_status: bool = True,
) -> DataFrame | Tuple[DataFrame, Optional[ValidationResult]]:
    """Streaming counterpart to :func:`read_from_data_product`.

    .. deprecated:: 0.0
       Use :func:`read_stream_with_governance` and resolve bindings through governance.
    """

    _warn_deprecated("read_stream_from_data_product", "read_stream_with_governance")

    return read_stream_with_contract(
        spark=spark,
        contract_service=contract_service,
        expected_contract_version=expected_contract_version,
        data_product_service=data_product_service,
        data_product_input=data_product_input,
        format=format,
        path=path,
        table=table,
        options=options,
        enforce=enforce,
        auto_cast=auto_cast,
        data_quality_service=data_quality_service,
        governance_service=governance_service,
        dataset_locator=dataset_locator,
        status_strategy=status_strategy,
        pipeline_context=pipeline_context,
        return_status=return_status,
    )



@dataclass
class WriteExecutionResult:
    """Return value produced by write executors."""

    result: ValidationResult
    status: Optional[ValidationResult]
    streaming_queries: list[Any]


class BaseWriteExecutor:
    """Shared implementation for batch and streaming contract writes."""

    streaming: bool = False

    def __init__(
        self,
        *,
        df: DataFrame,
        contract_id: Optional[str],
        contract_service: Optional[ContractServiceClient],
        expected_contract_version: Optional[str],
        path: Optional[str],
        table: Optional[str],
        format: Optional[str],
        options: Optional[Dict[str, str]],
        mode: str,
        enforce: bool,
        auto_cast: bool,
        data_quality_service: Optional[DataQualityServiceClient],
        governance_service: Optional[GovernanceServiceClient],
        data_product_service: Optional[DataProductServiceClient],
        data_product_output: Optional[DataProductOutputBinding | Mapping[str, object]],
        dataset_locator: Optional[DatasetLocatorStrategy],
        pipeline_context: Optional[PipelineContextLike],
        violation_strategy: Optional[WriteViolationStrategy],
        streaming_intervention_strategy: Optional[StreamingInterventionStrategy],
        streaming_batch_callback: Optional[Callable[[Mapping[str, Any]], None]] = None,
        publication_mode: GovernancePublicationMode | str | None = None,
        plan: Optional[ResolvedWritePlan] = None,
    ) -> None:
        self.df = df
        self.plan = plan
        resolved_contract_id = contract_id
        resolved_contract_version = expected_contract_version
        resolved_format = format
        if plan is not None:
            if plan.contract_id:
                resolved_contract_id = plan.contract_id
            if plan.contract_version:
                resolved_contract_version = plan.contract_version
            if plan.dataset_format and resolved_format is None:
                resolved_format = plan.dataset_format
        self.contract_id = resolved_contract_id
        self.contract_service = contract_service
        self.expected_contract_version = resolved_contract_version
        self.path = path
        self.table = table
        self.format = resolved_format
        self.options = dict(options or {})
        self.mode = mode
        self.enforce = enforce
        self.auto_cast = auto_cast
        self.data_quality_service = data_quality_service
        self.governance_service = governance_service
        self.data_product_service = data_product_service
        binding = normalise_output_binding(data_product_output)
        if plan is not None and plan.output_binding is not None:
            binding = plan.output_binding
        self.dp_output_binding = binding
        self.locator = dataset_locator or ContractFirstDatasetLocator()
        if pipeline_context is not None:
            self.pipeline_context = pipeline_context
        elif plan is not None:
            self.pipeline_context = plan.pipeline_context
        else:
            self.pipeline_context = None
        self.publication_mode = self._resolve_publication_mode(
            spark=df.sparkSession,
            override=publication_mode,
        )
        self.open_data_lineage_only = (
            self.publication_mode is GovernancePublicationMode.OPEN_DATA_LINEAGE
        )
        self.open_telemetry_only = (
            self.publication_mode is GovernancePublicationMode.OPEN_TELEMETRY
        )
        self._skip_governance_activity = self.open_data_lineage_only or self.open_telemetry_only
        self._last_write_resolution: Optional[DatasetResolution] = None
        strategy = violation_strategy or NoOpWriteViolationStrategy()
        self.data_product_status_enforce = enforce
        if plan is not None:
            strategy, status_enforce = _apply_plan_data_product_policy(
                strategy,
                plan,
                default_enforce=enforce,
            )
            self.data_product_status_enforce = status_enforce
        self.strategy = strategy
        self.streaming_intervention_strategy = streaming_intervention_strategy
        self.streaming_batch_callback = streaming_batch_callback

    @staticmethod
    def _resolve_publication_mode(
        *,
        spark: SparkSession,
        override: GovernancePublicationMode | str | None,
    ) -> GovernancePublicationMode:
        config: Dict[str, str] | None = None
        try:
            spark_conf = spark.conf  # type: ignore[attr-defined]
        except Exception:  # pragma: no cover - Spark may be absent in unit tests
            spark_conf = None
        if spark_conf is not None:
            for key in (
                "dc43.governance.publicationMode",
                "dc43.governance.publication_mode",
                "governance.publication.mode",
            ):
                try:
                    value = spark_conf.get(key)
                except Exception:  # pragma: no cover - SparkConf guards may throw
                    value = None
                if value:
                    config = {key: value}
                    break
        return resolve_publication_mode(explicit=override, config=config)

    def execute(self) -> WriteExecutionResult:
        df = self.df
        contract_id = self.contract_id
        contract_service = self.contract_service
        expected_contract_version = self.expected_contract_version
        path = self.path
        table = self.table
        format = self.format
        options = dict(self.options)
        mode = self.mode
        enforce = self.enforce
        auto_cast = self.auto_cast
        data_quality_service = self.data_quality_service
        governance_service = self.governance_service
        data_product_service = self.data_product_service
        dp_output_binding = self.dp_output_binding
        locator = self.locator
        strategy = self.strategy
        pipeline_context = self.pipeline_context
        streaming_intervention_strategy = self.streaming_intervention_strategy

        dp_service = data_product_service
        resolved_contract_id = contract_id
        resolved_expected_version = expected_contract_version
        governance_plan = self.plan
        if governance_plan is not None:
            if governance_plan.contract_id:
                resolved_contract_id = governance_plan.contract_id
            if governance_plan.contract_version:
                resolved_expected_version = governance_plan.contract_version
        if (
            resolved_contract_id is None
            and dp_service is not None
            and dp_output_binding is not None
            and dp_output_binding.data_product
            and dp_output_binding.port_name
        ):
            product: Optional[OpenDataProductStandard]
            try:
                product = _select_data_product(
                    service=dp_service,
                    data_product_id=dp_output_binding.data_product,
                    version_spec=dp_output_binding.data_product_version,
                    handler=strategy,
                    enforce=enforce,
                    operation="write",
                    status_enforce=self.data_product_status_enforce,
                )
            except ValueError:
                if enforce:
                    raise
                product = None
            if product is not None:
                port = product.find_output_port(dp_output_binding.port_name)
                if port is None:
                    message = (
                        f"Data product {dp_output_binding.data_product} output port {dp_output_binding.port_name}"
                        " is not defined"
                    )
                    if enforce:
                        raise ValueError(message)
                    logger.warning(message)
                else:
                    resolved_contract_id = port.contract_id
                    resolved_expected_version = port.version
                    logger.info(
                        "Resolved contract %s:%s from data product %s output %s",
                        resolved_contract_id,
                        resolved_expected_version,
                        dp_output_binding.data_product,
                        dp_output_binding.port_name,
                    )

        if (
            resolved_contract_id is None
            and dp_service is not None
            and dp_output_binding is not None
            and dp_output_binding.data_product
            and dp_output_binding.port_name
        ):
            try:
                contract_ref = dp_service.resolve_output_contract(
                    data_product_id=dp_output_binding.data_product,
                    port_name=dp_output_binding.port_name,
                )
            except Exception:  # pragma: no cover - defensive logging
                logger.exception(
                    "Failed to resolve output contract for data product %s port %s",
                    dp_output_binding.data_product,
                    dp_output_binding.port_name,
                )
            else:
                if contract_ref is None:
                    logger.warning(
                        "Data product %s output port %s did not provide a contract reference",
                        dp_output_binding.data_product,
                        dp_output_binding.port_name,
                    )
                else:
                    resolved_contract_id, resolved_expected_version = contract_ref
                    logger.info(
                        "Resolved contract %s:%s from data product %s output %s",
                        resolved_contract_id,
                        resolved_expected_version,
                        dp_output_binding.data_product,
                        dp_output_binding.port_name,
                    )
        elif (
            resolved_contract_id is None
            and dp_output_binding is not None
            and dp_output_binding.data_product
            and not dp_output_binding.port_name
        ):
            logger.warning(
                "data_product_output for %s cannot resolve a contract without port_name",
                dp_output_binding.data_product,
            )

        if resolved_contract_id is not None:
            contract_id = resolved_contract_id
        if expected_contract_version is None and resolved_expected_version is not None:
            expected_contract_version = resolved_expected_version

        contract: Optional[OpenDataContractStandard] = None
        if governance_plan is not None:
            contract = governance_plan.contract
            ensure_version(contract)
            _check_contract_version(resolved_expected_version, contract.version)
            _enforce_contract_status(
                handler=strategy,
                contract=contract,
                enforce=enforce,
                operation="write",
            )
        elif contract_id:
            contract = _resolve_contract(
                contract_id=contract_id,
                expected_version=expected_contract_version,
                service=contract_service,
                governance=_as_governance_service(governance_service),
            )
            ensure_version(contract)
            _check_contract_version(expected_contract_version, contract.version)
            _enforce_contract_status(
                handler=strategy,
                contract=contract,
                enforce=enforce,
                operation="write",
            )

        original_path = path
        original_table = table
        original_format = format

        resolution = locator.for_write(
            contract=contract,
            df=df,
            format=format,
            path=path,
            table=table,
        )
        self._last_write_resolution = resolution
        path = resolution.path
        table = resolution.table
        format = resolution.format
        plan_dataset_id: Optional[str] = None
        plan_dataset_version: Optional[str] = None
        plan_dataset_format: Optional[str] = None
        if governance_plan is not None:
            plan_dataset_id = governance_plan.dataset_id or None
            plan_dataset_version = governance_plan.dataset_version or None
            plan_dataset_format = governance_plan.dataset_format or None
        dataset_id = resolution.dataset_id or dataset_id_from_ref(table=table, path=path)
        if dataset_id is None:
            dataset_id = plan_dataset_id
        dataset_version = resolution.dataset_version or plan_dataset_version
        if plan_dataset_format:
            format = plan_dataset_format

        pre_validation_warnings: list[str] = []
        if contract:
            c_path, c_table = _ref_from_contract(contract)
            c_fmt = contract.servers[0].format if contract.servers else None
            if original_path and c_path and not _paths_compatible(original_path, c_path):
                message = f"Provided path {original_path} does not match contract server path {c_path}"
                logger.warning(message)
                pre_validation_warnings.append(message)
            if original_table and c_table and original_table != c_table:
                logger.warning(
                    "Provided table %s does not match contract server table %s",
                    original_table,
                    c_table,
                )
            if original_format and c_fmt and original_format != c_fmt:
                message = f"Format {original_format} does not match contract server format {c_fmt}"
                logger.warning(message)
                pre_validation_warnings.append(message)
            if format is None:
                format = c_fmt

        out_df = df
        try:
            is_streaming = bool(df.isStreaming)  # type: ignore[attr-defined]
        except AttributeError:
            is_streaming = False
        streaming_active = self.streaming or is_streaming
        if streaming_active and not self.streaming:
            logger.info("Detected streaming dataframe; enabling streaming mode")
        if streaming_active and not dataset_version:
            dataset_version = _timestamp()
        dataset_details: Dict[str, Any] = {}
        if dataset_id:
            dataset_details["dataset_id"] = dataset_id
        if dataset_version:
            dataset_details["dataset_version"] = dataset_version
        governance_client = _as_governance_service(governance_service)
        result = ValidationResult(ok=True, errors=[], warnings=[], metrics={})
        observed_schema: Optional[Dict[str, Dict[str, Any]]] = None
        observed_metrics: Optional[Dict[str, Any]] = None
        expectation_plan: list[Mapping[str, Any]] = []
        assessment: Optional[QualityAssessment] = None
        base_pipeline_context = normalise_pipeline_context(pipeline_context)
        if contract:
            if data_quality_service is None and governance_client is None:
                raise ValueError(
                    "data_quality_service or governance_service is required when validating against a contract",
                )
            cid, cver = contract_identity(contract)
            logger.info("Writing with contract %s:%s", cid, cver)
            if data_quality_service is not None:
                expectation_plan = list(
                    data_quality_service.describe_expectations(contract=contract)
                )
            elif governance_client is not None:
                expectation_plan = list(
                    governance_client.describe_expectations(
                        contract_id=cid,
                        contract_version=cver,
                    )
                )
            observed_schema, observed_metrics = collect_observations(
                df,
                contract,
                expectations=expectation_plan,
                collect_metrics=not streaming_active,
            )
            dq_validation: Optional[ValidationResult] = None
            if data_quality_service is not None:
                dq_validation = _evaluate_with_service(
                    contract=contract,
                    service=data_quality_service,
                    schema=observed_schema,
                    metrics=observed_metrics,
                )
                result = dq_validation
            if governance_client is not None:

                def _observations() -> ObservationPayload:
                    return ObservationPayload(
                        metrics=dict(observed_metrics or {}),
                        schema=dict(observed_schema or {}),
                        reused=True,
                    )

                if governance_plan is not None:
                    assessment = governance_client.evaluate_write_plan(
                        plan=governance_plan,
                        validation=dq_validation,
                        observations=_observations,
                    )
                else:
                    assessment = governance_client.evaluate_dataset(
                        contract_id=cid,
                        contract_version=cver,
                        dataset_id=dataset_id,
                        dataset_version=dataset_version or "",
                        validation=dq_validation,
                        observations=_observations,
                        pipeline_context=base_pipeline_context,
                        operation="write",
                        draft_on_violation=False,
                    )
                result = assessment.validation or assessment.status or dq_validation or ValidationResult(
                    ok=True,
                    errors=[],
                    warnings=[],
                    metrics={},
                    schema={},
                )
            if dataset_details:
                result.merge_details(dataset_details)
            if streaming_active and observed_metrics == {}:
                logger.info(
                    "Streaming write for %s:%s validated without collecting Spark metrics",
                    cid,
                    cver,
                )
            if pre_validation_warnings:
                for warning in pre_validation_warnings:
                    if warning not in result.warnings:
                        result.warnings.append(warning)
            logger.info(
                "Write validation: ok=%s errors=%s warnings=%s",
                result.ok,
                result.errors,
                result.warnings,
            )
            out_df = apply_contract(df, contract, auto_cast=auto_cast)
            if format and c_fmt and format != c_fmt:
                msg = f"Format {format} does not match contract server format {c_fmt}"
                logger.warning(msg)
                result.warnings.append(msg)
            if path and c_path and not _paths_compatible(path, c_path):
                msg = f"Path {path} does not match contract server path {c_path}"
                logger.warning(msg)
                result.warnings.append(msg)
            if not result.ok and enforce:
                raise ValueError(f"Contract validation failed: {result.errors}")

        def _register_output_port_if_needed() -> None:
            if dp_service is None or dp_output_binding is None or contract is None:
                return
            if not dp_output_binding.data_product:
                logger.warning(
                    "data_product_output requires a data_product identifier to register output ports",
                )
                return
            port_name = dp_output_binding.port_name or contract.id
            try:
                registration = dp_service.register_output_port(
                    data_product_id=dp_output_binding.data_product,
                    port_name=port_name,
                    contract_id=contract.id,
                    contract_version=contract.version,
                    bump=dp_output_binding.bump,
                    custom_properties=dp_output_binding.custom_properties,
                )
            except Exception:  # pragma: no cover - defensive logging
                logger.exception(
                    "Failed to register data product output port %s on %s",
                    port_name,
                    dp_output_binding.data_product,
                )
            else:
                if registration.changed:
                    product = registration.product
                    version = product.version or "<unknown>"
                    if (product.status or "").lower() != "draft":
                        raise RuntimeError(
                            "Data product output registration did not produce a draft version"
                        )
                    raise RuntimeError(
                        f"Data product {dp_output_binding.data_product} output port {port_name} "
                        f"requires review at version {version}"
                    )
                product = registration.product
                requested_version = dp_output_binding.data_product_version
                matched_spec = False
                if requested_version:
                    resolved_product, matched_spec = _load_binding_product_version(
                        service=dp_service,
                        data_product_id=dp_output_binding.data_product,
                        version_spec=requested_version,
                        enforce=enforce,
                        operation="output",
                    )
                    if resolved_product is not None:
                        product = resolved_product
                _enforce_data_product_status(
                    handler=strategy,
                    data_product=product,
                    enforce=self.data_product_status_enforce,
                    operation="write",
                )
                if dp_output_binding.data_product_version and matched_spec:
                    _check_data_product_version(
                        expected=dp_output_binding.data_product_version,
                        actual=product.version,
                        data_product_id=dp_output_binding.data_product,
                        subject="Data product",
                        enforce=enforce,
                    )

        options_dict: Dict[str, str] = {}
        if resolution.write_options:
            options_dict.update(resolution.write_options)
        if options:
            options_dict.update(options)
        expectation_predicates: Mapping[str, str] = {}
        predicates = result.details.get("expectation_predicates")
        if isinstance(predicates, Mapping):
            expectation_predicates = dict(predicates)

        if contract:

            def revalidator(new_df: DataFrame) -> ValidationResult:  # type: ignore[misc]
                schema, metrics = collect_observations(
                    new_df,
                    contract,
                    expectations=expectation_plan,
                    collect_metrics=not streaming_active,
                )
                if data_quality_service is not None:
                    return _evaluate_with_service(
                        contract=contract,
                        service=data_quality_service,
                        schema=schema,
                        metrics=metrics,
                    )
                if governance_client is not None:

                    def _observations() -> ObservationPayload:
                        return ObservationPayload(
                            metrics=dict(metrics or {}),
                            schema=dict(schema or {}),
                            reused=True,
                        )

                    follow_up = governance_client.evaluate_dataset(
                        contract_id=contract.id,
                        contract_version=contract.version,
                        dataset_id=dataset_id,
                        dataset_version=dataset_version or "",
                        validation=None,
                        observations=_observations,
                        pipeline_context=base_pipeline_context,
                        operation="write",
                        draft_on_violation=False,
                    )
                    return follow_up.validation or follow_up.status or ValidationResult(
                        ok=True,
                        errors=[],
                        warnings=[],
                        metrics={},
                        schema={},
                    )
                return ValidationResult(ok=True, errors=[], warnings=[], metrics={}, schema={})

        else:

            def revalidator(new_df: DataFrame) -> ValidationResult:  # type: ignore[misc]
                return ValidationResult(
                    ok=True,
                    errors=[],
                    warnings=[],
                    metrics={},
                    schema={},
                )

        observation_writer: Optional[StreamingObservationWriter] = None
        checkpoint_option = None
        if options_dict:
            checkpoint_option = options_dict.get("checkpointLocation")
        if streaming_active and contract:
            observation_writer = StreamingObservationWriter(
                contract=contract,
                expectation_plan=expectation_plan,
                data_quality_service=data_quality_service,
                dataset_id=dataset_id,
                dataset_version=dataset_version,
                enforce=enforce,
                checkpoint_location=checkpoint_option,
                intervention=streaming_intervention_strategy,
                progress_callback=self.streaming_batch_callback,
            )
            observation_writer.attach_validation(result)

        context = WriteStrategyContext(
            df=df,
            aligned_df=out_df,
            contract=contract,
            path=path,
            table=table,
            format=format,
            options=options_dict,
            mode=mode,
            validation=result,
            dataset_id=dataset_id,
            dataset_version=dataset_version,
            revalidate=revalidator,
            expectation_predicates=expectation_predicates,
            pipeline_context=base_pipeline_context,
            streaming=streaming_active,
            streaming_observation_writer=observation_writer,
        )
        violation_plan = strategy.plan(context)

        requests: list[WriteRequest] = []
        primary_status: Optional[ValidationResult] = None
        validations: list[ValidationResult] = []
        streaming_queries: list[Any] = []
        status_records: list[tuple[Optional[ValidationResult], WriteRequest]] = []

        def _extend_plan(request: WriteRequest) -> None:
            requests.append(request)

        if violation_plan.primary is not None:
            _extend_plan(violation_plan.primary)
        for extra in violation_plan.additional:
            _extend_plan(extra)

        if not requests:
            final_result = (
                violation_plan.result_factory()
                if violation_plan.result_factory
                else result
            )
            _register_output_port_if_needed()
            return WriteExecutionResult(final_result, None, [])

        request_warning_messages: list[str] = []

        for index, request in enumerate(requests):
            for message in request.warnings:
                if message not in request_warning_messages:
                    request_warning_messages.append(message)
            status, request_validation, handles = _execute_write_request(
                request,
                governance_client=governance_client,
                enforce=enforce,
            )
            if handles:
                streaming_queries.extend(handles)
            if status and expectation_plan and "expectation_plan" not in status.details:
                status.merge_details({"expectation_plan": expectation_plan})
            status_records.append((status, request))
            if request_validation is not None:
                validations.append(request_validation)
            if index == 0:
                primary_status = status

        if violation_plan.result_factory is not None:
            final_result = violation_plan.result_factory()
        elif validations:
            final_result = validations[0]
        else:
            final_result = result

        if request_warning_messages:
            for message in request_warning_messages:
                if message not in final_result.warnings:
                    final_result.warnings.append(message)

        if status_records:
            aggregated_entries: list[Dict[str, Any]] = []
            aggregated_violations = 0
            aggregated_draft: Optional[str] = None
            merged_warnings: list[str] = []
            merged_errors: list[str] = []

            for index, (status, request) in enumerate(status_records):
                if status is None:
                    continue

                details = dict(status.details or {})
                dataset_ref = request.dataset_id or dataset_id_from_ref(
                    table=request.table,
                    path=request.path,
                )
                entry: Dict[str, Any] = {
                    "role": "primary" if index == 0 else "auxiliary",
                    "dataset_id": dataset_ref,
                    "dataset_version": request.dataset_version,
                    "status": status.status,
                }
                if request.path:
                    entry["path"] = request.path
                if request.table:
                    entry["table"] = request.table
                if status.reason:
                    entry["reason"] = status.reason
                if details:
                    entry["details"] = details
                aggregated_entries.append(entry)

                violations = details.get("violations")
                if isinstance(violations, (int, float)):
                    aggregated_violations = max(aggregated_violations, int(violations))
                draft_version = details.get("draft_contract_version")
                if isinstance(draft_version, str) and not aggregated_draft:
                    aggregated_draft = draft_version
                merged_warnings.extend(details.get("warnings", []) or [])
                merged_errors.extend(details.get("errors", []) or [])

                if request.warnings:
                    for message in request.warnings:
                        if message not in merged_warnings:
                            merged_warnings.append(message)
                        if message not in status.warnings:
                            status.warnings.append(message)
                    entry_warnings = list(details.get("warnings", []) or [])
                    for message in request.warnings:
                        if message not in entry_warnings:
                            entry_warnings.append(message)
                    if entry_warnings:
                        details["warnings"] = entry_warnings

            if aggregated_entries:
                if primary_status is None:
                    primary_status = next(
                        (status for status, _ in status_records if status is not None),
                        None,
                    )
                if primary_status is not None:
                    primary_details = dict(primary_status.details or {})
                    primary_details.setdefault("auxiliary_statuses", aggregated_entries)
                    primary_entry = next(
                        (entry for entry in aggregated_entries if entry.get("role") == "primary"),
                        None,
                    )
                    if aggregated_violations:
                        primary_details["violations"] = aggregated_violations
                    if aggregated_draft and not primary_details.get("draft_contract_version"):
                        primary_details["draft_contract_version"] = aggregated_draft

                    aux_statuses = [
                        str(entry.get("status", "")).lower()
                        for entry in aggregated_entries
                        if entry.get("role") != "primary"
                    ]
                    original_status = primary_status.status
                    override_note: Optional[str] = None
                    if isinstance(original_status, str) and original_status.lower() == "block":
                        if any(status in {"ok", "warn", "warning"} for status in aux_statuses):
                            override_note = (
                                "Primary DQ status downgraded after split outputs succeeded"
                            )
                    if override_note:
                        primary_details.setdefault("warnings", []).append(override_note)
                        primary_details.setdefault("overrides", []).append(override_note)
                        primary_status.warnings.append(override_note)
                        primary_details.setdefault(
                            "status_before_override",
                            original_status,
                        )
                        primary_status.status = "warn"
                        primary_details["status"] = "warn"
                    if primary_entry is not None:
                        primary_entry["status"] = primary_status.status
                        entry_details = dict(primary_entry.get("details") or {})
                        for key, value in primary_details.items():
                            if key == "auxiliary_statuses":
                                continue
                            entry_details[key] = value
                        primary_entry["details"] = entry_details
                        primary_details.setdefault("dataset_id", primary_entry.get("dataset_id"))
                        primary_details.setdefault("dataset_version", primary_entry.get("dataset_version"))
                    primary_status.details = primary_details

                merged_warnings.extend(final_result.warnings)
                merged_errors.extend(final_result.errors)
                if merged_warnings:
                    final_result.details.setdefault("warnings", merged_warnings)
                if merged_errors:
                    final_result.details.setdefault("errors", merged_errors)

        _register_output_port_if_needed()

        if (
            governance_plan is not None
            and governance_client is not None
            and assessment is not None
            and not self._skip_governance_activity
        ):
            try:
                governance_client.register_write_activity(
                    plan=governance_plan, assessment=assessment
                )
            except RuntimeError:
                raise
            except ValueError as exc:
                if self.enforce:
                    raise
                logger.warning("Governance write activity rejected: %s", exc)
            except Exception:  # pragma: no cover - defensive logging
                logger.exception(
                    "Failed to register governance write activity for %s",
                    governance_plan.contract_id,
                )
        if self.open_data_lineage_only and governance_client is not None:
            try:
                resolution = self._last_write_resolution
                dataset_format = format
                dataset_table = table
                dataset_path = path
                if resolution is not None:
                    if resolution.format:
                        dataset_format = resolution.format
                    if resolution.table:
                        dataset_table = resolution.table
                    if resolution.path:
                        dataset_path = resolution.path
                lineage_contract_id = None
                lineage_contract_version = None
                if contract is not None:
                    lineage_contract_id = contract.id
                    lineage_contract_version = contract.version
                else:
                    lineage_contract_id = resolved_contract_id
                    lineage_contract_version = resolved_expected_version
                lineage_event = build_lineage_run_event(
                    operation="write",
                    plan=governance_plan,
                    pipeline_context=self.pipeline_context,
                    contract_id=lineage_contract_id,
                    contract_version=lineage_contract_version,
                    dataset_id=dataset_id,
                    dataset_version=dataset_version,
                    dataset_format=dataset_format,
                    table=dataset_table,
                    path=dataset_path,
                    binding=self.dp_output_binding,
                    validation=result,
                    status=primary_status,
                    expectation_plan=expectation_plan,
                )
                governance_client.publish_lineage_event(event=lineage_event)
            except Exception:  # pragma: no cover - defensive logging
                logger.exception(
                    "Failed to publish lineage run for %s",
                    lineage_contract_id,
                )
        if self.open_telemetry_only:
            try:
                resolution = self._last_write_resolution
                dataset_format = format
                dataset_table = table
                dataset_path = path
                if resolution is not None:
                    if resolution.format:
                        dataset_format = resolution.format
                    if resolution.table:
                        dataset_table = resolution.table
                    if resolution.path:
                        dataset_path = resolution.path
                telemetry_contract_id = contract.id if contract is not None else resolved_contract_id
                telemetry_contract_version = (
                    contract.version if contract is not None else resolved_expected_version
                )
                telemetry_dataset_id = plan_dataset_id or dataset_id
                telemetry_dataset_version = plan_dataset_version or dataset_version
                telemetry_dataset_format = plan_dataset_format or dataset_format
                record_telemetry_span(
                    operation="write",
                    plan=governance_plan,
                    pipeline_context=self.pipeline_context,
                    contract_id=telemetry_contract_id,
                    contract_version=telemetry_contract_version,
                    dataset_id=telemetry_dataset_id,
                    dataset_version=telemetry_dataset_version,
                    dataset_format=telemetry_dataset_format,
                    table=dataset_table,
                    path=dataset_path,
                    binding=self.dp_output_binding,
                    validation=result,
                    status=primary_status,
                    expectation_plan=expectation_plan,
                )
            except Exception:  # pragma: no cover - defensive logging
                logger.exception(
                    "Failed to record telemetry span for %s",
                    resolved_contract_id,
                )

        if streaming_queries:
            final_result.merge_details({"streaming_queries": streaming_queries})
            if primary_status is not None:
                primary_status.merge_details({"streaming_queries": streaming_queries})

        return WriteExecutionResult(final_result, primary_status, streaming_queries)


class BatchWriteExecutor(BaseWriteExecutor):
    """Batch-only write execution."""


class StreamingWriteExecutor(BaseWriteExecutor):
    """Streaming write execution."""

    streaming = True


def _execute_write(
    executor_cls: Type[BaseWriteExecutor],
    *,
    df: DataFrame,
    contract_id: Optional[str],
    contract_service: Optional[ContractServiceClient],
    expected_contract_version: Optional[str],
    path: Optional[str],
    table: Optional[str],
    format: Optional[str],
    options: Optional[Dict[str, str]],
    mode: str,
    enforce: bool,
    auto_cast: bool,
    data_quality_service: Optional[DataQualityServiceClient],
    governance_service: Optional[GovernanceServiceClient],
    data_product_service: Optional[DataProductServiceClient],
    data_product_output: Optional[DataProductOutputBinding | Mapping[str, object]],
    dataset_locator: Optional[DatasetLocatorStrategy],
    pipeline_context: Optional[PipelineContextLike],
    publication_mode: GovernancePublicationMode | str | None,
    return_status: bool,
    violation_strategy: Optional[WriteViolationStrategy],
    streaming_intervention_strategy: Optional[StreamingInterventionStrategy],
    streaming_batch_callback: Optional[Callable[[Mapping[str, Any]], None]] = None,
    plan: Optional[ResolvedWritePlan] = None,
) -> ValidationResult | tuple[ValidationResult, Optional[ValidationResult]]:
    executor = executor_cls(
        df=df,
        contract_id=contract_id,
        contract_service=contract_service,
        expected_contract_version=expected_contract_version,
        path=path,
        table=table,
        format=format,
        options=options,
        mode=mode,
        enforce=enforce,
        auto_cast=auto_cast,
        data_quality_service=data_quality_service,
        governance_service=governance_service,
        data_product_service=data_product_service,
        data_product_output=data_product_output,
        dataset_locator=dataset_locator,
        pipeline_context=pipeline_context,
        violation_strategy=violation_strategy,
        streaming_intervention_strategy=streaming_intervention_strategy,
        streaming_batch_callback=streaming_batch_callback,
        publication_mode=publication_mode,
        plan=plan,
    )
    execution = executor.execute()
    result = execution.result
    status = execution.status
    if return_status:
        return result, status
    return result


@overload
def write_with_contract(
    *,
    df: DataFrame,
    contract_id: Optional[str] = None,
    contract_service: Optional[ContractServiceClient] = None,
    expected_contract_version: Optional[str] = None,
    path: Optional[str] = None,
    table: Optional[str] = None,
    format: Optional[str] = None,
    options: Optional[Dict[str, str]] = None,
    mode: str = "append",
    enforce: bool = True,
    auto_cast: bool = True,
    data_quality_service: Optional[DataQualityServiceClient] = None,
    governance_service: Optional[GovernanceServiceClient] = None,
    data_product_service: Optional[DataProductServiceClient] = None,
    data_product_output: Optional[DataProductOutputBinding | Mapping[str, object]] = None,
    dataset_locator: Optional[DatasetLocatorStrategy] = None,
    pipeline_context: Optional[PipelineContextLike] = None,
    return_status: Literal[True],
    violation_strategy: Optional[WriteViolationStrategy] = None,
) -> tuple[ValidationResult, Optional[ValidationResult]]:
    ...


@overload
def write_with_contract(
    *,
    df: DataFrame,
    contract_id: Optional[str] = None,
    contract_service: Optional[ContractServiceClient] = None,
    expected_contract_version: Optional[str] = None,
    path: Optional[str] = None,
    table: Optional[str] = None,
    format: Optional[str] = None,
    options: Optional[Dict[str, str]] = None,
    mode: str = "append",
    enforce: bool = True,
    auto_cast: bool = True,
    data_quality_service: Optional[DataQualityServiceClient] = None,
    governance_service: Optional[GovernanceServiceClient] = None,
    data_product_service: Optional[DataProductServiceClient] = None,
    data_product_output: Optional[DataProductOutputBinding | Mapping[str, object]] = None,
    dataset_locator: Optional[DatasetLocatorStrategy] = None,
    pipeline_context: Optional[PipelineContextLike] = None,
    return_status: Literal[False] = False,
    violation_strategy: Optional[WriteViolationStrategy] = None,
) -> ValidationResult:
    ...


@overload
def write_with_contract(
    *,
    df: DataFrame,
    contract_id: Optional[str] = None,
    contract_service: Optional[ContractServiceClient] = None,
    expected_contract_version: Optional[str] = None,
    path: Optional[str] = None,
    table: Optional[str] = None,
    format: Optional[str] = None,
    options: Optional[Dict[str, str]] = None,
    mode: str = "append",
    enforce: bool = True,
    auto_cast: bool = True,
    data_quality_service: Optional[DataQualityServiceClient] = None,
    governance_service: Optional[GovernanceServiceClient] = None,
    data_product_service: Optional[DataProductServiceClient] = None,
    data_product_output: Optional[DataProductOutputBinding | Mapping[str, object]] = None,
    dataset_locator: Optional[DatasetLocatorStrategy] = None,
    pipeline_context: Optional[PipelineContextLike] = None,
    return_status: bool = False,
    violation_strategy: Optional[WriteViolationStrategy] = None,
) -> ValidationResult | tuple[ValidationResult, Optional[ValidationResult]]:
    ...


def write_with_contract(
    *,
    df: DataFrame,
    contract_id: Optional[str] = None,
    contract_service: Optional[ContractServiceClient] = None,
    expected_contract_version: Optional[str] = None,
    path: Optional[str] = None,
    table: Optional[str] = None,
    format: Optional[str] = None,
    options: Optional[Dict[str, str]] = None,
    mode: str = "append",
    enforce: bool = True,
    auto_cast: bool = True,
    data_quality_service: Optional[DataQualityServiceClient] = None,
    governance_service: Optional[GovernanceServiceClient] = None,
    data_product_service: Optional[DataProductServiceClient] = None,
    data_product_output: Optional[DataProductOutputBinding | Mapping[str, object]] = None,
    dataset_locator: Optional[DatasetLocatorStrategy] = None,
    pipeline_context: Optional[PipelineContextLike] = None,
    return_status: bool = False,
    violation_strategy: Optional[WriteViolationStrategy] = None,
) -> ValidationResult | tuple[ValidationResult, Optional[ValidationResult]]:
    """Write a batch ``DataFrame`` with contract enforcement.

    .. deprecated:: 0.0
       Use :func:`write_with_governance` with a :class:`GovernanceSparkWriteRequest`.
    """

    _warn_deprecated("write_with_contract", "write_with_governance")

    return _execute_write(
        BatchWriteExecutor,
        df=df,
        contract_id=contract_id,
        contract_service=contract_service,
        expected_contract_version=expected_contract_version,
        path=path,
        table=table,
        format=format,
        options=options,
        mode=mode,
        enforce=enforce,
        auto_cast=auto_cast,
        data_quality_service=data_quality_service,
        governance_service=governance_service,
        data_product_service=data_product_service,
        data_product_output=data_product_output,
        dataset_locator=dataset_locator,
        pipeline_context=pipeline_context,
        publication_mode=None,
        return_status=return_status,
        violation_strategy=violation_strategy,
        streaming_intervention_strategy=None,
    )


@overload
def write_with_governance(
    *,
    df: DataFrame,
    request: GovernanceSparkWriteRequest | Mapping[str, object],
    governance_service: GovernanceServiceClient,
    enforce: bool = True,
    auto_cast: bool = True,
    return_status: Literal[True],
    violation_strategy: Optional[WriteViolationStrategy] = None,
) -> tuple[ValidationResult, Optional[ValidationResult]]:
    ...


@overload
def write_with_governance(
    *,
    df: DataFrame,
    request: GovernanceSparkWriteRequest | Mapping[str, object],
    governance_service: GovernanceServiceClient,
    enforce: bool = True,
    auto_cast: bool = True,
    return_status: Literal[False] = False,
    violation_strategy: Optional[WriteViolationStrategy] = None,
) -> ValidationResult:
    ...


@overload
def write_with_governance(
    *,
    df: DataFrame,
    request: GovernanceSparkWriteRequest | Mapping[str, object],
    governance_service: GovernanceServiceClient,
    enforce: bool = True,
    auto_cast: bool = True,
    return_status: bool = False,
    violation_strategy: Optional[WriteViolationStrategy] = None,
) -> ValidationResult | tuple[ValidationResult, Optional[ValidationResult]]:
    ...


def write_with_governance(
    *,
    df: DataFrame,
    request: GovernanceSparkWriteRequest | Mapping[str, object],
    governance_service: GovernanceServiceClient,
    enforce: bool = True,
    auto_cast: bool = True,
    return_status: bool = False,
    violation_strategy: Optional[WriteViolationStrategy] = None,
) -> ValidationResult | tuple[ValidationResult, Optional[ValidationResult]]:
    """Write a batch ``DataFrame`` relying solely on the governance client."""

    if not isinstance(request, GovernanceSparkWriteRequest):
        if isinstance(request, Mapping):
            request = GovernanceSparkWriteRequest(**dict(request))
        else:
            raise TypeError("request must be a GovernanceSparkWriteRequest or mapping")

    context = request.context
    if context.enforce_data_product_status is None:
        context.enforce_data_product_status = bool(enforce)

    plan = governance_service.resolve_write_context(context=context)
    pipeline_ctx = request.context.pipeline_context or plan.pipeline_context

    return _execute_write(
        BatchWriteExecutor,
        df=df,
        contract_id=plan.contract_id,
        contract_service=None,
        expected_contract_version=plan.contract_version,
        path=request.path,
        table=request.table,
        format=request.format,
        options=request.options,
        mode=request.mode,
        enforce=enforce,
        auto_cast=auto_cast,
        data_quality_service=None,
        governance_service=governance_service,
        data_product_service=None,
        data_product_output=None,
        dataset_locator=request.dataset_locator,
        pipeline_context=pipeline_ctx,
        publication_mode=request.publication_mode,
        return_status=return_status,
        violation_strategy=violation_strategy,
        streaming_intervention_strategy=None,
        plan=plan,
    )


@overload
def write_stream_with_contract(
    *,
    df: DataFrame,
    contract_id: Optional[str] = None,
    contract_service: Optional[ContractServiceClient] = None,
    expected_contract_version: Optional[str] = None,
    path: Optional[str] = None,
    table: Optional[str] = None,
    format: Optional[str] = None,
    options: Optional[Dict[str, str]] = None,
    mode: str = "append",
    enforce: bool = True,
    auto_cast: bool = True,
    data_quality_service: Optional[DataQualityServiceClient] = None,
    governance_service: Optional[GovernanceServiceClient] = None,
    data_product_service: Optional[DataProductServiceClient] = None,
    data_product_output: Optional[DataProductOutputBinding | Mapping[str, object]] = None,
    dataset_locator: Optional[DatasetLocatorStrategy] = None,
    pipeline_context: Optional[PipelineContextLike] = None,
    return_status: Literal[True],
    violation_strategy: Optional[WriteViolationStrategy] = None,
    streaming_intervention_strategy: Optional[StreamingInterventionStrategy] = None,
    on_streaming_batch: Optional[Callable[[Mapping[str, Any]], None]] = None,
) -> tuple[ValidationResult, Optional[ValidationResult]]:
    ...


@overload
def write_stream_with_contract(
    *,
    df: DataFrame,
    contract_id: Optional[str] = None,
    contract_service: Optional[ContractServiceClient] = None,
    expected_contract_version: Optional[str] = None,
    path: Optional[str] = None,
    table: Optional[str] = None,
    format: Optional[str] = None,
    options: Optional[Dict[str, str]] = None,
    mode: str = "append",
    enforce: bool = True,
    auto_cast: bool = True,
    data_quality_service: Optional[DataQualityServiceClient] = None,
    governance_service: Optional[GovernanceServiceClient] = None,
    data_product_service: Optional[DataProductServiceClient] = None,
    data_product_output: Optional[DataProductOutputBinding | Mapping[str, object]] = None,
    dataset_locator: Optional[DatasetLocatorStrategy] = None,
    pipeline_context: Optional[PipelineContextLike] = None,
    return_status: Literal[False] = False,
    violation_strategy: Optional[WriteViolationStrategy] = None,
    streaming_intervention_strategy: Optional[StreamingInterventionStrategy] = None,
    on_streaming_batch: Optional[Callable[[Mapping[str, Any]], None]] = None,
) -> ValidationResult:
    ...


@overload
def write_stream_with_contract(
    *,
    df: DataFrame,
    contract_id: Optional[str] = None,
    contract_service: Optional[ContractServiceClient] = None,
    expected_contract_version: Optional[str] = None,
    path: Optional[str] = None,
    table: Optional[str] = None,
    format: Optional[str] = None,
    options: Optional[Dict[str, str]] = None,
    mode: str = "append",
    enforce: bool = True,
    auto_cast: bool = True,
    data_quality_service: Optional[DataQualityServiceClient] = None,
    governance_service: Optional[GovernanceServiceClient] = None,
    data_product_service: Optional[DataProductServiceClient] = None,
    data_product_output: Optional[DataProductOutputBinding | Mapping[str, object]] = None,
    dataset_locator: Optional[DatasetLocatorStrategy] = None,
    pipeline_context: Optional[PipelineContextLike] = None,
    return_status: bool = False,
    violation_strategy: Optional[WriteViolationStrategy] = None,
    streaming_intervention_strategy: Optional[StreamingInterventionStrategy] = None,
    on_streaming_batch: Optional[Callable[[Mapping[str, Any]], None]] = None,
) -> ValidationResult | tuple[ValidationResult, Optional[ValidationResult]]:
    ...


def write_stream_with_contract(
    *,
    df: DataFrame,
    contract_id: Optional[str] = None,
    contract_service: Optional[ContractServiceClient] = None,
    expected_contract_version: Optional[str] = None,
    path: Optional[str] = None,
    table: Optional[str] = None,
    format: Optional[str] = None,
    options: Optional[Dict[str, str]] = None,
    mode: str = "append",
    enforce: bool = True,
    auto_cast: bool = True,
    data_quality_service: Optional[DataQualityServiceClient] = None,
    governance_service: Optional[GovernanceServiceClient] = None,
    data_product_service: Optional[DataProductServiceClient] = None,
    data_product_output: Optional[DataProductOutputBinding | Mapping[str, object]] = None,
    dataset_locator: Optional[DatasetLocatorStrategy] = None,
    pipeline_context: Optional[PipelineContextLike] = None,
    return_status: bool = False,
    violation_strategy: Optional[WriteViolationStrategy] = None,
    streaming_intervention_strategy: Optional[StreamingInterventionStrategy] = None,
    on_streaming_batch: Optional[Callable[[Mapping[str, Any]], None]] = None,
) -> ValidationResult | tuple[ValidationResult, Optional[ValidationResult]]:
    """Write a streaming ``DataFrame`` with contract enforcement.

    .. deprecated:: 0.0
       Use :func:`write_stream_with_governance` with a :class:`GovernanceSparkWriteRequest`.
    """

    _warn_deprecated("write_stream_with_contract", "write_stream_with_governance")

    return _execute_write(
        StreamingWriteExecutor,
        df=df,
        contract_id=contract_id,
        contract_service=contract_service,
        expected_contract_version=expected_contract_version,
        path=path,
        table=table,
        format=format,
        options=options,
        mode=mode,
        enforce=enforce,
        auto_cast=auto_cast,
        data_quality_service=data_quality_service,
        governance_service=governance_service,
        data_product_service=data_product_service,
        data_product_output=data_product_output,
        dataset_locator=dataset_locator,
        pipeline_context=pipeline_context,
        publication_mode=None,
        return_status=return_status,
        violation_strategy=violation_strategy,
        streaming_intervention_strategy=streaming_intervention_strategy,
        streaming_batch_callback=on_streaming_batch,
    )


@overload
def write_stream_with_governance(
    *,
    df: DataFrame,
    request: GovernanceSparkWriteRequest | Mapping[str, object],
    governance_service: GovernanceServiceClient,
    enforce: bool = True,
    auto_cast: bool = True,
    return_status: Literal[True],
    violation_strategy: Optional[WriteViolationStrategy] = None,
    streaming_intervention_strategy: Optional[StreamingInterventionStrategy] = None,
    on_streaming_batch: Optional[Callable[[Mapping[str, Any]], None]] = None,
) -> tuple[ValidationResult, Optional[ValidationResult]]:
    ...


@overload
def write_stream_with_governance(
    *,
    df: DataFrame,
    request: GovernanceSparkWriteRequest | Mapping[str, object],
    governance_service: GovernanceServiceClient,
    enforce: bool = True,
    auto_cast: bool = True,
    return_status: Literal[False] = False,
    violation_strategy: Optional[WriteViolationStrategy] = None,
    streaming_intervention_strategy: Optional[StreamingInterventionStrategy] = None,
    on_streaming_batch: Optional[Callable[[Mapping[str, Any]], None]] = None,
) -> ValidationResult:
    ...


@overload
def write_stream_with_governance(
    *,
    df: DataFrame,
    request: GovernanceSparkWriteRequest | Mapping[str, object],
    governance_service: GovernanceServiceClient,
    enforce: bool = True,
    auto_cast: bool = True,
    return_status: bool = False,
    violation_strategy: Optional[WriteViolationStrategy] = None,
    streaming_intervention_strategy: Optional[StreamingInterventionStrategy] = None,
    on_streaming_batch: Optional[Callable[[Mapping[str, Any]], None]] = None,
) -> ValidationResult | tuple[ValidationResult, Optional[ValidationResult]]:
    ...


def write_stream_with_governance(
    *,
    df: DataFrame,
    request: GovernanceSparkWriteRequest | Mapping[str, object],
    governance_service: GovernanceServiceClient,
    enforce: bool = True,
    auto_cast: bool = True,
    return_status: bool = False,
    violation_strategy: Optional[WriteViolationStrategy] = None,
    streaming_intervention_strategy: Optional[StreamingInterventionStrategy] = None,
    on_streaming_batch: Optional[Callable[[Mapping[str, Any]], None]] = None,
) -> ValidationResult | tuple[ValidationResult, Optional[ValidationResult]]:
    """Stream a ``DataFrame`` using only the governance client."""

    if not isinstance(request, GovernanceSparkWriteRequest):
        if isinstance(request, Mapping):
            request = GovernanceSparkWriteRequest(**dict(request))
        else:
            raise TypeError("request must be a GovernanceSparkWriteRequest or mapping")

    plan = governance_service.resolve_write_context(context=request.context)
    pipeline_ctx = request.context.pipeline_context or plan.pipeline_context

    return _execute_write(
        StreamingWriteExecutor,
        df=df,
        contract_id=plan.contract_id,
        contract_service=None,
        expected_contract_version=plan.contract_version,
        path=request.path,
        table=request.table,
        format=request.format,
        options=request.options,
        mode=request.mode,
        enforce=enforce,
        auto_cast=auto_cast,
        data_quality_service=None,
        governance_service=governance_service,
        data_product_service=None,
        data_product_output=None,
        dataset_locator=request.dataset_locator,
        pipeline_context=pipeline_ctx,
        publication_mode=request.publication_mode,
        return_status=return_status,
        violation_strategy=violation_strategy,
        streaming_intervention_strategy=streaming_intervention_strategy,
        streaming_batch_callback=on_streaming_batch,
        plan=plan,
    )










def write_with_contract_id(
    *,
    df: DataFrame,
    contract_id: str,
    contract_service: ContractServiceClient,
    expected_contract_version: Optional[str] = None,
    path: Optional[str] = None,
    table: Optional[str] = None,
    format: Optional[str] = None,
    options: Optional[Dict[str, str]] = None,
    mode: str = "append",
    enforce: bool = True,
    auto_cast: bool = True,
    data_quality_service: Optional[DataQualityServiceClient] = None,
    governance_service: Optional[GovernanceServiceClient] = None,
    dataset_locator: Optional[DatasetLocatorStrategy] = None,
    pipeline_context: Optional[PipelineContextLike] = None,
    return_status: bool = False,
    violation_strategy: Optional[WriteViolationStrategy] = None,
) -> ValidationResult | tuple[ValidationResult, Optional[ValidationResult]]:
    """Write a dataset by referencing a contract identifier directly.

    .. deprecated:: 0.0
       Use :func:`write_with_governance`.
    """

    _warn_deprecated("write_with_contract_id", "write_with_governance")

    return write_with_contract(
        df=df,
        contract_id=contract_id,
        contract_service=contract_service,
        expected_contract_version=expected_contract_version,
        path=path,
        table=table,
        format=format,
        options=options,
        mode=mode,
        enforce=enforce,
        auto_cast=auto_cast,
        data_quality_service=data_quality_service,
        governance_service=governance_service,
        dataset_locator=dataset_locator,
        pipeline_context=pipeline_context,
        return_status=return_status,
        violation_strategy=violation_strategy,
    )


def write_stream_with_contract_id(
    *,
    df: DataFrame,
    contract_id: str,
    contract_service: ContractServiceClient,
    expected_contract_version: Optional[str] = None,
    path: Optional[str] = None,
    table: Optional[str] = None,
    format: Optional[str] = None,
    options: Optional[Dict[str, str]] = None,
    mode: str = "append",
    enforce: bool = True,
    auto_cast: bool = True,
    data_quality_service: Optional[DataQualityServiceClient] = None,
    governance_service: Optional[GovernanceServiceClient] = None,
    dataset_locator: Optional[DatasetLocatorStrategy] = None,
    pipeline_context: Optional[PipelineContextLike] = None,
    return_status: bool = False,
    violation_strategy: Optional[WriteViolationStrategy] = None,
    streaming_intervention_strategy: Optional[StreamingInterventionStrategy] = None,
    on_streaming_batch: Optional[Callable[[Mapping[str, Any]], None]] = None,
) -> ValidationResult | tuple[ValidationResult, Optional[ValidationResult]]:
    """Streaming counterpart to :func:`write_with_contract_id`.

    .. deprecated:: 0.0
       Use :func:`write_stream_with_governance`.
    """

    _warn_deprecated("write_stream_with_contract_id", "write_stream_with_governance")

    return write_stream_with_contract(
        df=df,
        contract_id=contract_id,
        contract_service=contract_service,
        expected_contract_version=expected_contract_version,
        path=path,
        table=table,
        format=format,
        options=options,
        mode=mode,
        enforce=enforce,
        auto_cast=auto_cast,
        data_quality_service=data_quality_service,
        governance_service=governance_service,
        dataset_locator=dataset_locator,
        pipeline_context=pipeline_context,
        return_status=return_status,
        violation_strategy=violation_strategy,
        streaming_intervention_strategy=streaming_intervention_strategy,
        on_streaming_batch=on_streaming_batch,
    )


def write_to_data_product(
    *,
    df: DataFrame,
    data_product_service: DataProductServiceClient,
    data_product_output: DataProductOutputBinding | Mapping[str, object],
    contract_id: Optional[str] = None,
    contract_service: Optional[ContractServiceClient] = None,
    expected_contract_version: Optional[str] = None,
    path: Optional[str] = None,
    table: Optional[str] = None,
    format: Optional[str] = None,
    options: Optional[Dict[str, str]] = None,
    mode: str = "append",
    enforce: bool = True,
    auto_cast: bool = True,
    data_quality_service: Optional[DataQualityServiceClient] = None,
    governance_service: Optional[GovernanceServiceClient] = None,
    dataset_locator: Optional[DatasetLocatorStrategy] = None,
    pipeline_context: Optional[PipelineContextLike] = None,
    return_status: bool = False,
    violation_strategy: Optional[WriteViolationStrategy] = None,
) -> ValidationResult | tuple[ValidationResult, Optional[ValidationResult]]:
    """Write a dataset using a data product output binding.

    .. deprecated:: 0.0
       Use :func:`write_with_governance` and register bindings via governance.
    """

    _warn_deprecated("write_to_data_product", "write_with_governance")

    return write_with_contract(
        df=df,
        contract_id=contract_id,
        contract_service=contract_service,
        expected_contract_version=expected_contract_version,
        data_product_service=data_product_service,
        data_product_output=data_product_output,
        path=path,
        table=table,
        format=format,
        options=options,
        mode=mode,
        enforce=enforce,
        auto_cast=auto_cast,
        data_quality_service=data_quality_service,
        governance_service=governance_service,
        dataset_locator=dataset_locator,
        pipeline_context=pipeline_context,
        return_status=return_status,
        violation_strategy=violation_strategy,
    )


def write_stream_to_data_product(
    *,
    df: DataFrame,
    data_product_service: DataProductServiceClient,
    data_product_output: DataProductOutputBinding | Mapping[str, object],
    contract_id: Optional[str] = None,
    contract_service: Optional[ContractServiceClient] = None,
    expected_contract_version: Optional[str] = None,
    path: Optional[str] = None,
    table: Optional[str] = None,
    format: Optional[str] = None,
    options: Optional[Dict[str, str]] = None,
    mode: str = "append",
    enforce: bool = True,
    auto_cast: bool = True,
    data_quality_service: Optional[DataQualityServiceClient] = None,
    governance_service: Optional[GovernanceServiceClient] = None,
    dataset_locator: Optional[DatasetLocatorStrategy] = None,
    pipeline_context: Optional[PipelineContextLike] = None,
    return_status: bool = False,
    violation_strategy: Optional[WriteViolationStrategy] = None,
    streaming_intervention_strategy: Optional[StreamingInterventionStrategy] = None,
    on_streaming_batch: Optional[Callable[[Mapping[str, Any]], None]] = None,
) -> ValidationResult | tuple[ValidationResult, Optional[ValidationResult]]:
    """Streaming counterpart to :func:`write_to_data_product`.

    .. deprecated:: 0.0
       Use :func:`write_stream_with_governance` and register bindings via governance.
    """

    _warn_deprecated("write_stream_to_data_product", "write_stream_with_governance")

    return write_stream_with_contract(
        df=df,
        contract_id=contract_id,
        contract_service=contract_service,
        expected_contract_version=expected_contract_version,
        data_product_service=data_product_service,
        data_product_output=data_product_output,
        path=path,
        table=table,
        format=format,
        options=options,
        mode=mode,
        enforce=enforce,
        auto_cast=auto_cast,
        data_quality_service=data_quality_service,
        governance_service=governance_service,
        dataset_locator=dataset_locator,
        pipeline_context=pipeline_context,
        return_status=return_status,
        violation_strategy=violation_strategy,
        streaming_intervention_strategy=streaming_intervention_strategy,
        on_streaming_batch=on_streaming_batch,
    )
def _execute_write_request(
    request: WriteRequest,
    *,
    governance_client: Optional[GovernanceServiceClient],
    enforce: bool,
) -> tuple[Optional[ValidationResult], Optional[ValidationResult], list[Any]]:
    df_to_write = request.df
    checkpointed = False
    streaming_handles: list[Any] = []
    mode = (request.mode or "").lower()
    should_checkpoint = (
        not request.streaming and request.path and mode == "overwrite"
    )
    if should_checkpoint and not _supports_dataframe_checkpointing(df_to_write):
        logger.info(
            "Skipping dataframe checkpoint for %s because caching is unsupported",
            request.path,
        )
        should_checkpoint = False

    if request.streaming:
        pass
    elif should_checkpoint:
        try:
            df_to_write = df_to_write.localCheckpoint(eager=True)
        except Exception:  # pragma: no cover - defensive fallback
            logger.exception(
                "Failed to checkpoint dataframe prior to overwrite for %s",
                request.path,
            )
        else:
            checkpointed = True

    validation = request.validation_factory() if request.validation_factory else None
    observation_scope = "streaming_batch" if request.streaming else "pre_write_dataframe"
    _annotate_observation_scope(
        validation,
        operation="write",
        scope=observation_scope,
    )
    observation_writer = request.streaming_observation_writer
    if observation_writer is not None and validation is not None:
        observation_writer.attach_validation(validation)

    if request.streaming:
        metrics_query = None
        if observation_writer is not None and not observation_writer.active:
            metrics_mode = request.mode or "append"
            metrics_query = observation_writer.start(
                df_to_write,
                output_mode=metrics_mode,
            )

        writer = df_to_write.writeStream
        if request.format:
            writer = writer.format(request.format)
        if request.options:
            writer = writer.options(**request.options)
        if request.mode:
            writer = writer.outputMode(request.mode)
        if request.table:
            logger.info("Starting streaming write to table %s", request.table)
            streaming_query = writer.toTable(request.table)
            streaming_handles.append(streaming_query)
            if observation_writer is not None:
                observation_writer.watch_sink_query(streaming_query)
        else:
            target = request.path
            if target:
                logger.info("Starting streaming write to path %s", target)
                streaming_query = writer.start(target)
                streaming_handles.append(streaming_query)
                if observation_writer is not None:
                    observation_writer.watch_sink_query(streaming_query)
            else:
                logger.info("Starting streaming write with implicit sink")
                streaming_query = writer.start()
                streaming_handles.append(streaming_query)
                if observation_writer is not None:
                    observation_writer.watch_sink_query(streaming_query)

        if metrics_query is not None:
            streaming_handles.append(metrics_query)
    else:
        writer = df_to_write.write
        if request.format:
            writer = writer.format(request.format)
        if request.options:
            writer = writer.options(**request.options)
        writer = writer.mode(request.mode)

        if request.table:
            logger.info("Writing dataframe to table %s", request.table)
            writer.saveAsTable(request.table)
        else:
            if not request.path:
                raise ValueError("Either table or path must be provided for write")
            logger.info("Writing dataframe to path %s", request.path)
            writer.save(request.path)
    expectation_plan: list[Mapping[str, Any]] = []
    if validation is not None:
        raw_plan = validation.details.get("expectation_plan")
        if isinstance(raw_plan, Iterable):
            expectation_plan = [
                item for item in raw_plan if isinstance(item, Mapping)
            ]
    if validation is not None and request.warnings:
        for message in request.warnings:
            if message not in validation.warnings:
                validation.warnings.append(message)
    contract = request.contract
    status: Optional[ValidationResult] = None
    if governance_client and contract and validation is not None:
        dq_dataset_id = request.dataset_id or dataset_id_from_ref(
            table=request.table,
            path=request.path,
        )
        dq_dataset_version = (
            request.dataset_version
            or get_delta_version(
                df_to_write.sparkSession,
                table=request.table,
                path=request.path,
            )
            or "unknown"
        )
        if request.streaming and dq_dataset_version == "unknown":
            dq_dataset_version = _timestamp()
        request.dataset_id = dq_dataset_id
        request.dataset_version = dq_dataset_version

        dataset_details = {
            "dataset_id": dq_dataset_id,
            "dataset_version": dq_dataset_version,
        }

        def _post_write_observations() -> ObservationPayload:
            metrics, schema_payload, reused_metrics = build_metrics_payload(
                df_to_write,
                contract,
                validation=validation,
                include_schema=True,
                expectations=expectation_plan,
                collect_metrics=not request.streaming,
            )
            if reused_metrics:
                logger.info(
                    "Using cached validation metrics for %s@%s",
                    dq_dataset_id,
                    dq_dataset_version,
                )
            elif request.streaming:
                logger.info(
                    "Streaming write for %s@%s defers Spark metric collection",
                    dq_dataset_id,
                    dq_dataset_version,
                )
            else:
                logger.info(
                    "Computing DQ metrics for %s@%s after write",
                    dq_dataset_id,
                    dq_dataset_version,
                )
            return ObservationPayload(
                metrics=metrics,
                schema=schema_payload,
                reused=reused_metrics,
            )

        cid, cver = contract_identity(contract)

        assessment = governance_client.evaluate_dataset(
            contract_id=cid,
            contract_version=cver,
            dataset_id=dq_dataset_id,
            dataset_version=dq_dataset_version,
            validation=validation,
            observations=_post_write_observations,
            pipeline_context=request.pipeline_context,
            operation="write",
        )
        status = assessment.status
        if validation is not None:
            validation.merge_details(dataset_details)
        if status:
            logger.info(
                "DQ status for %s@%s after write: %s",
                dq_dataset_id,
                dq_dataset_version,
                status.status,
            )
            status.merge_details(dataset_details)
            _annotate_observation_scope(
                status,
                operation="write",
                scope=observation_scope,
            )
            if enforce and status.status == "block":
                details_snapshot: Dict[str, Any] = dict(status.details or {})
                if status.reason:
                    details_snapshot.setdefault("reason", status.reason)
                raise ValueError(f"DQ violation: {details_snapshot or status.status}")

        request_draft = False
        if not validation.ok:
            request_draft = True
        elif status and status.status not in (None, "ok"):
            request_draft = True

        if request_draft:
            draft_contract = governance_client.review_validation_outcome(
                validation=validation,
                base_contract=contract,
                dataset_id=dq_dataset_id,
                dataset_version=dq_dataset_version,
                data_format=request.format,
                dq_status=status,
                draft_requested=True,
                pipeline_context=request.pipeline_context,
                operation="write",
            )
            if draft_contract is not None and status is not None:
                details = dict(status.details or {})
                details.setdefault("draft_contract_version", draft_contract.version)
                status.details = details

        if assessment.draft and enforce:
            raise ValueError(
                "DQ governance returned a draft contract for the submitted dataset, "
                "indicating the provided contract version is out of date",
            )

        governance_client.link_dataset_contract(
            dataset_id=dq_dataset_id,
            dataset_version=dq_dataset_version,
            contract_id=contract.id,
            contract_version=contract.version,
        )

    try:
        return status, validation, streaming_handles
    finally:
        if checkpointed:
            try:
                df_to_write.unpersist()
            except Exception:  # pragma: no cover - defensive cleanup
                logger.exception(
                    "Failed to unpersist checkpointed dataframe for %s",
                    request.path,
                )
