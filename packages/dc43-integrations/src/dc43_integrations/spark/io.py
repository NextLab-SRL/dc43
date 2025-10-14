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
)
import logging
import tempfile
from dataclasses import dataclass, field
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
from dc43_service_clients.governance import PipelineContext, normalise_pipeline_context
from .data_quality import (
    build_metrics_payload,
    collect_observations,
)
from .validation import apply_contract
from dc43_service_backends.core.odcs import contract_identity, custom_properties_dict, ensure_version
from dc43_service_backends.core.versioning import SemVer
from open_data_contract_standard.model import OpenDataContractStandard, Server  # type: ignore

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
                stop = getattr(query, "stop", None)
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

        result = _evaluate_with_service(
            contract=self.contract,
            service=self.data_quality_service,
            schema=schema,
            metrics=metrics,
            reused=False,
        )
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
        self._notify_progress(
            {
                "type": "observer-started",
                "query_name": getattr(query, "name", self.query_name),
                "id": getattr(query, "id", ""),
            }
        )
        return query

logger = logging.getLogger(__name__)


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
    ) -> tuple[Optional[str], Optional[str], Optional[str], Optional[Server]]:
        server: Optional[Server] = None
        if contract and contract.servers:
            c_path, c_table = _ref_from_contract(contract)
            server = contract.servers[0]
            c_format = getattr(server, "format", None)
            if c_path is not None:
                path = c_path
            if c_table is not None:
                table = c_table
            if c_format is not None and format is None:
                format = c_format
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
        path, table, format, _ = self._resolve_base(contract, path=path, table=table, format=format)
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
        path, table, format, _ = self._resolve_base(contract, path=path, table=table, format=format)
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
    path = getattr(server, "path", None)
    if path:
        return path, None
    # Build table name from catalog/schema/database/dataset parts when present
    last = getattr(server, "dataset", None) or getattr(server, "database", None)
    parts = [
        getattr(server, "catalog", None),
        getattr(server, "schema_", None),
        last,
    ]
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
    service: ContractServiceClient,
) -> OpenDataContractStandard:
    """Fetch a contract from the configured service respecting version hints."""

    if service is None:
        raise ValueError("contract_service is required when contract_id is provided")

    if not expected_version:
        contract = service.latest(contract_id)
        if contract is None:
            raise ValueError(f"No versions available for contract {contract_id}")
        return contract

    if expected_version.startswith("=="):
        version = expected_version[2:]
        return service.get(contract_id, version)

    if expected_version.startswith(">="):
        base = expected_version[2:]
        version = _select_version(list(service.list_versions(contract_id)), base)
        return service.get(contract_id, version)

    return service.get(contract_id, expected_version)


def _enforce_contract_status(
    *,
    handler: object,
    contract: OpenDataContractStandard,
    enforce: bool,
    operation: str,
) -> None:
    """Apply a contract status policy defined by ``handler``."""

    validator = getattr(handler, "validate_contract_status", None)
    if validator is None:
        _validate_contract_status(
            contract=contract,
            enforce=enforce,
            operation=operation,
        )
        return

    validator(contract=contract, enforce=enforce, operation=operation)


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

    raw_status = getattr(contract, "status", None)
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
            contract_id=str(getattr(contract, "id", "")),
            contract_version=str(getattr(contract, "version", "")),
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
        contract_id=str(getattr(contract, "id", "")),
        contract_version=str(getattr(contract, "version", "")),
        status=status_value,
        operation=operation,
    )
    if enforce:
        raise ValueError(message)
    logger.warning(message)



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
    ) -> None:
        self.spark = spark
        self.contract_id = contract_id
        self.contract_service = contract_service
        self.expected_contract_version = expected_contract_version
        self.user_format = format
        self.user_path = path
        self.user_table = table
        self.options = dict(options or {})
        self.enforce = enforce
        self.auto_cast = auto_cast
        self.data_quality_service = data_quality_service
        self.governance_service = governance_service
        self.data_product_service = data_product_service
        self.dp_binding = normalise_input_binding(data_product_input)
        self.locator = dataset_locator or ContractFirstDatasetLocator()
        self.status_handler = status_strategy or DefaultReadStatusStrategy()
        self.pipeline_context = pipeline_context

    def execute(self) -> tuple[DataFrame, Optional[ValidationResult]]:
        """Execute the read pipeline and return the dataframe/status pair."""

        contract = self._resolve_contract()
        resolution = self._resolve_resolution(contract)
        dataframe = self._load_dataframe(resolution)
        streaming_active = self._detect_streaming(dataframe)
        dataset_id, dataset_version = self._dataset_identity(resolution, streaming_active)
        dataframe, validation, expectation_plan, contract_identity_tuple = self._apply_contract(
            dataframe,
            contract,
            dataset_id,
            dataset_version,
            streaming_active,
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
        streaming_active = self.streaming or bool(getattr(dataframe, "isStreaming", False))
        if streaming_active and not self.streaming:
            logger.info("Detected streaming dataframe; enabling streaming mode")
        return streaming_active

    def _dataset_identity(
        self,
        resolution: DatasetResolution,
        streaming_active: bool,
    ) -> tuple[str, str]:
        dataset_id = resolution.dataset_id or dataset_id_from_ref(
            table=resolution.table,
            path=resolution.path,
        )
        observed_version = (
            resolution.dataset_version
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
    ) -> tuple[
        DataFrame,
        Optional[ValidationResult],
        list[Mapping[str, Any]],
        Optional[tuple[str, str]],
    ]:
        if contract is None:
            return dataframe, None, [], None
        if self.data_quality_service is None:
            raise ValueError(
                "data_quality_service is required when validating against a contract"
            )

        cid, cver = contract_identity(contract)
        logger.info("Reading with contract %s:%s", cid, cver)
        expectation_plan = list(
            self.data_quality_service.describe_expectations(contract=contract)
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

        validation = _evaluate_with_service(
            contract=contract,
            service=self.data_quality_service,
            schema=observed_schema,
            metrics=observed_metrics,
        )
        self._normalise_streaming_validation(validation, streaming_active=streaming_active)
        validation.merge_details({
            "dataset_id": dataset_id,
            "dataset_version": dataset_version,
        })
        if expectation_plan and "expectation_plan" not in validation.details:
            validation.merge_details({"expectation_plan": expectation_plan})
        logger.info(
            "Read validation: ok=%s errors=%s warnings=%s",
            validation.ok,
            validation.errors,
            validation.warnings,
        )
        if not validation.ok and self.enforce:
            raise ValueError(f"Contract validation failed: {validation.errors}")

        dataframe = apply_contract(dataframe, contract, auto_cast=self.auto_cast)
        return dataframe, validation, expectation_plan, (cid, cver)

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
    ) -> Optional[ValidationResult]:
        governance_client = _as_governance_service(self.governance_service)
        if (
            governance_client is None
            or contract is None
            or validation is None
            or contract_identity_tuple is None
        ):
            return None

        cid, cver = contract_identity_tuple
        base_pipeline_context = normalise_pipeline_context(self.pipeline_context)

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
                logger.info("Using cached validation metrics for %s@%s", dataset_id, dataset_version)
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
        )
        status = assessment.status
        if status:
            logger.info("DQ status for %s@%s: %s", dataset_id, dataset_version, status.status)
            status.merge_details({
                "dataset_id": dataset_id,
                "dataset_version": dataset_version,
            })
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
    return_status: bool,
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
    """Read a batch DataFrame with contract enforcement and governance hooks."""

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
        return_status=return_status,
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
    """Create a streaming ``DataFrame`` while enforcing an ODCS contract."""

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
        return_status=return_status,
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
    """Read and validate a dataset by referencing a contract identifier directly."""

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
    """Streaming counterpart to :func:`read_from_contract`."""

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
    """Read a dataset by resolving the contract from a data product input binding."""

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
    """Streaming counterpart to :func:`read_from_data_product`."""

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
    ) -> None:
        self.df = df
        self.contract_id = contract_id
        self.contract_service = contract_service
        self.expected_contract_version = expected_contract_version
        self.path = path
        self.table = table
        self.format = format
        self.options = dict(options or {})
        self.mode = mode
        self.enforce = enforce
        self.auto_cast = auto_cast
        self.data_quality_service = data_quality_service
        self.governance_service = governance_service
        self.data_product_service = data_product_service
        self.dp_output_binding = normalise_output_binding(data_product_output)
        self.locator = dataset_locator or ContractFirstDatasetLocator()
        self.pipeline_context = pipeline_context
        self.strategy = violation_strategy or NoOpWriteViolationStrategy()
        self.streaming_intervention_strategy = streaming_intervention_strategy
        self.streaming_batch_callback = streaming_batch_callback

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
        if contract_id:
            contract = _resolve_contract(
                contract_id=contract_id,
                expected_version=expected_contract_version,
                service=contract_service,
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
        path = resolution.path
        table = resolution.table
        format = resolution.format
        dataset_id = resolution.dataset_id or dataset_id_from_ref(table=table, path=path)
        dataset_version = resolution.dataset_version

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
        streaming_active = self.streaming or bool(getattr(df, "isStreaming", False))
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
        if contract:
            if data_quality_service is None:
                raise ValueError(
                    "data_quality_service is required when validating against a contract"
                )
            cid, cver = contract_identity(contract)
            logger.info("Writing with contract %s:%s", cid, cver)
            expectation_plan = list(
                data_quality_service.describe_expectations(contract=contract)
            )
            observed_schema, observed_metrics = collect_observations(
                df,
                contract,
                expectations=expectation_plan,
                collect_metrics=not streaming_active,
            )
            result = _evaluate_with_service(
                contract=contract,
                service=data_quality_service,
                schema=observed_schema,
                metrics=observed_metrics,
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
                return _evaluate_with_service(
                    contract=contract,
                    service=data_quality_service,
                    schema=schema,
                    metrics=metrics,
                )

        else:

            def revalidator(new_df: DataFrame) -> ValidationResult:  # type: ignore[misc]
                return ValidationResult(
                    ok=True,
                    errors=[],
                    warnings=[],
                    metrics={},
                    schema={},
                )

        base_pipeline_context = normalise_pipeline_context(pipeline_context)

        observation_writer: Optional[StreamingObservationWriter] = None
        checkpoint_option = None
        if options_dict:
            checkpoint_option = options_dict.get("checkpointLocation")
        if streaming_active and contract and data_quality_service is not None:
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
        plan = strategy.plan(context)

        requests: list[WriteRequest] = []
        primary_status: Optional[ValidationResult] = None
        validations: list[ValidationResult] = []
        streaming_queries: list[Any] = []
        status_records: list[tuple[Optional[ValidationResult], WriteRequest]] = []

        def _extend_plan(request: WriteRequest) -> None:
            requests.append(request)

        if plan.primary is not None:
            _extend_plan(plan.primary)
        for extra in plan.additional:
            _extend_plan(extra)

        if not requests:
            final_result = plan.result_factory() if plan.result_factory else result
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

        if plan.result_factory is not None:
            final_result = plan.result_factory()
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
    return_status: bool,
    violation_strategy: Optional[WriteViolationStrategy],
    streaming_intervention_strategy: Optional[StreamingInterventionStrategy],
    streaming_batch_callback: Optional[Callable[[Mapping[str, Any]], None]] = None,
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
    """Write a batch ``DataFrame`` with contract enforcement."""

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
        return_status=return_status,
        violation_strategy=violation_strategy,
        streaming_intervention_strategy=None,
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
    """Write a streaming ``DataFrame`` with contract enforcement."""

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
        return_status=return_status,
        violation_strategy=violation_strategy,
        streaming_intervention_strategy=streaming_intervention_strategy,
        streaming_batch_callback=on_streaming_batch,
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
    """Write a dataset by referencing a contract identifier directly."""

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
    """Streaming counterpart to :func:`write_with_contract_id`."""

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
    """Write a dataset using a data product output binding."""

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
    """Streaming counterpart to :func:`write_to_data_product`."""

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
    if request.streaming:
        pass
    elif request.path and request.mode.lower() == "overwrite":
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
