from __future__ import annotations

from typing import (
    Any,
    Callable,
    Dict,
    Iterable,
    Mapping,
    Optional,
    Sequence,
    Union,
    Tuple,
    TypeVar,
)
import warnings
from dataclasses import dataclass
from datetime import datetime, timezone

from pyspark.sql import DataFrame, SparkSession

from dc43_service_clients.data_quality import (
    ObservationPayload,
    ValidationResult,
)
from dc43_service_clients.data_quality.client.interface import DataQualityServiceClient
from dc43_service_clients.governance import (
    PipelineContext,
    GovernancePublicationMode,
)
from dc43_service_clients.governance.models import (
    GovernanceReadContext,
    GovernanceWriteContext,
)
from open_data_contract_standard.model import OpenDataContractStandard

PipelineContextLike = Union[
    PipelineContext,
    Mapping[str, object],
    Sequence[Tuple[str, object]],
    str,
]


def resolve_dataset_version(
    version_template: Optional[str],
    batch_id: Union[int, str] = "init",
    timestamp: Optional[datetime] = None,
) -> str:
    """Resolve a dynamic dataset version template with execution metrics."""
    if not version_template:
        return "unknown"
    if "{" not in version_template:
        return version_template

    ts = timestamp or datetime.now(timezone.utc)
    try:
        return version_template.format(
            batch_id=batch_id,
            timestamp=ts.strftime("%Y%m%d%H%M%S"),
            unix_timestamp=int(ts.timestamp()),
        )
    except KeyError:
        return version_template


@dataclass(slots=True)
class GovernanceSparkReadRequest:
    """Wrapper aggregating governance context and Spark-specific overrides."""

    context: GovernanceReadContext | Mapping[str, object]
    format: Optional[str] = None
    path: Optional[str] = None
    table: Optional[str] = None
    options: Optional[Mapping[str, str]] = None
    dataset_locator: Optional[Any] = None  # Typed as Any to avoid circular import with strategies
    status_strategy: Optional[Any] = None  # Typed as Any to avoid circular import
    pipeline_context: Optional[PipelineContextLike] = None
    publication_mode: GovernancePublicationMode | str | None = None

    def __post_init__(self) -> None:
        if not isinstance(self.context, GovernanceReadContext):
            if isinstance(self.context, Mapping):
                pass
            else:
                raise TypeError("context must be a GovernanceReadContext or mapping")
        if self.options is not None and not isinstance(self.options, dict):
            self.options = dict(self.options)
        if isinstance(self.publication_mode, str):
            self.publication_mode = GovernancePublicationMode.from_value(self.publication_mode)
        if self.pipeline_context is not None:
             if isinstance(self.context, GovernanceReadContext):
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
    dataset_locator: Optional[Any] = None # Typed as Any
    pipeline_context: Optional[PipelineContextLike] = None
    publication_mode: GovernancePublicationMode | str | None = None
    violation_strategy: Optional[Any] = None  # Typed as Any to avoid circular import
    streaming_intervention_strategy: Optional[Any] = None
    writer_modifier: Optional[Callable[[Any], Any]] = None
    observation_writer_modifier: Optional[Callable[[Any], Any]] = None

    def __post_init__(self) -> None:
        if not isinstance(self.context, GovernanceWriteContext):
            if isinstance(self.context, Mapping):
                 pass
            else:
                raise TypeError("context must be a GovernanceWriteContext or mapping")
        if self.options is not None and not isinstance(self.options, dict):
            self.options = dict(self.options)
        if isinstance(self.publication_mode, str):
            self.publication_mode = GovernancePublicationMode.from_value(self.publication_mode)
        if self.pipeline_context is not None:
             if isinstance(self.context, GovernanceWriteContext):
                self.context.pipeline_context = self.pipeline_context


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


def _normalise_path_ref(path: Optional[str | Iterable[str]]) -> Optional[str]:
    """Return a representative path from ``path``."""

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
) -> Tuple[Optional[str], Optional[str]]:
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
    result.merge_details(payload)


def _warn_deprecated(old_name: str, new_name: str) -> None:
    """Issue a deprecation warning for an old IO function."""
    warnings.warn(
        f"{old_name} is deprecated and will be removed in a future release. "
        f"Please use {new_name} instead.",
        DeprecationWarning,
        stacklevel=2,
    )


T = TypeVar("T")


def _as_governance_service(service: T | None) -> T | None:
    """Return the service instance or None."""
    return service
