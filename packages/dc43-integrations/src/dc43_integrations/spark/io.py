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
    Union,
    overload,
)
import logging
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession

from dc43_service_clients.contracts.client.interface import ContractServiceClient
from dc43_service_clients.data_quality.client.interface import DataQualityServiceClient
from dc43_service_clients.data_quality import ObservationPayload, ValidationResult
from dc43_service_clients.governance.client.interface import GovernanceServiceClient
from dc43_service_clients.governance import PipelineContext, normalise_pipeline_context
from .data_quality import (
    build_metrics_payload,
    collect_observations,
)
from .validation import apply_contract
from dc43.odcs import contract_identity, custom_properties_dict, ensure_version
from dc43.versioning import SemVer
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

    VERSIONING_PROPERTY = "dc43.versioning"

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
        if base.suffix:
            folder = base.parent / base.stem / self.dataset_version
            if self.subpath:
                folder = folder / self.subpath
            target = folder / base.name
            return str(target)

        folder = base / self.dataset_version
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

    def apply(
        self,
        *,
        dataframe: DataFrame,
        status: Optional[ValidationResult],
        enforce: bool,
        context: ReadStatusContext,
    ) -> tuple[DataFrame, Optional[ValidationResult]]:  # noqa: D401 - short docstring
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


# Overloads help type checkers infer the return type based on ``return_status``
# so callers can destructure the tuple without false positives.
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
    # Governance / DQ orchestration
    data_quality_service: Optional[DataQualityServiceClient] = None,
    governance_service: Optional[GovernanceServiceClient] = None,
    dataset_locator: Optional[DatasetLocatorStrategy] = None,
    status_strategy: Optional[ReadStatusStrategy] = None,
    pipeline_context: Optional[PipelineContextLike] = None,
    return_status: bool = True,
) -> DataFrame | Tuple[DataFrame, Optional[ValidationResult]]:
    """Read a DataFrame and validate/enforce an ODCS contract.

    - If ``contract_id`` is provided, the contract is fetched from
      ``contract_service`` before validating schema and aligning columns/types.
    - If ``governance_service`` is provided the remote coordinator evaluates
      metrics, records governance activity, and returns the dataset status when
      ``return_status=True``.
    """
    locator = dataset_locator or ContractFirstDatasetLocator()
    status_handler = status_strategy or DefaultReadStatusStrategy()

    contract: Optional[OpenDataContractStandard] = None
    if contract_id:
        contract = _resolve_contract(
            contract_id=contract_id,
            expected_version=expected_contract_version,
            service=contract_service,
        )
        ensure_version(contract)
        _check_contract_version(expected_contract_version, contract.version)

    original_path = path
    original_table = table
    original_format = format

    resolution = locator.for_read(
        contract=contract,
        spark=spark,
        format=format,
        path=path,
        table=table,
    )
    path = resolution.path
    table = resolution.table
    format = resolution.format

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
        if format is None:
            format = c_fmt

    if not table and not (path or resolution.load_paths):
        raise ValueError("Either table or path must be provided for read")

    reader = spark.read
    if format:
        reader = reader.format(format)
    option_map: Dict[str, str] = {}
    if resolution.read_options:
        option_map.update(resolution.read_options)
    if options:
        option_map.update(options)
    if option_map:
        reader = reader.options(**option_map)
    target = resolution.load_paths or path
    df = reader.table(table) if table else reader.load(target)
    result: Optional[ValidationResult] = None
    observed_schema: Optional[Dict[str, Dict[str, Any]]] = None
    observed_metrics: Optional[Dict[str, Any]] = None
    cid: Optional[str] = None
    cver: Optional[str] = None
    expectation_plan: list[Mapping[str, Any]] = []
    if contract:
        if data_quality_service is None:
            raise ValueError(
                "data_quality_service is required when validating against a contract"
            )
        cid, cver = contract_identity(contract)
        logger.info("Reading with contract %s:%s", cid, cver)
        expectation_plan = list(
            data_quality_service.describe_expectations(contract=contract)
        )
        observed_schema, observed_metrics = collect_observations(
            df,
            contract,
            expectations=expectation_plan,
        )
        result = _evaluate_with_service(
            contract=contract,
            service=data_quality_service,
            schema=observed_schema,
            metrics=observed_metrics,
        )
        if expectation_plan and "expectation_plan" not in result.details:
            result.merge_details({"expectation_plan": expectation_plan})
        logger.info(
            "Read validation: ok=%s errors=%s warnings=%s",
            result.ok,
            result.errors,
            result.warnings,
        )
        if not result.ok and enforce:
            raise ValueError(f"Contract validation failed: {result.errors}")
        df = apply_contract(df, contract, auto_cast=auto_cast)

    # DQ integration
    governance_client = _as_governance_service(governance_service)
    status: Optional[ValidationResult] = None
    if governance_client and contract and result is not None:
        ds_id = resolution.dataset_id or dataset_id_from_ref(table=table, path=path)
        ds_ver = (
            resolution.dataset_version
            or get_delta_version(spark, table=table, path=path)
            or "unknown"
        )

        base_pipeline_context = normalise_pipeline_context(pipeline_context)

        def _observations() -> ObservationPayload:
            metrics_payload, schema_payload, reused = build_metrics_payload(
                df,
                contract,
                validation=result,
                include_schema=True,
                expectations=expectation_plan,
            )
            if reused:
                logger.info("Using cached validation metrics for %s@%s", ds_id, ds_ver)
            else:
                logger.info("Computing DQ metrics for %s@%s", ds_id, ds_ver)
            return ObservationPayload(
                metrics=metrics_payload,
                schema=schema_payload,
                reused=reused,
            )

        assessment = governance_client.evaluate_dataset(
            contract_id=cid,
            contract_version=cver,
            dataset_id=ds_id,
            dataset_version=ds_ver,
            validation=result,
            observations=_observations,
            pipeline_context=base_pipeline_context,
            operation="read",
        )
        status = assessment.status
        if status:
            logger.info("DQ status for %s@%s: %s", ds_id, ds_ver, status.status)

        df, status = status_handler.apply(
            dataframe=df,
            status=status,
            enforce=enforce,
            context=ReadStatusContext(
                contract=contract,
                dataset_id=resolution.dataset_id,
                dataset_version=resolution.dataset_version,
            ),
        )

    return (df, status) if return_status else df


# Overloads allow static checkers to track the tuple return when ``return_status``
# is requested, avoiding "DataFrame is not iterable" warnings.
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
    dataset_locator: Optional[DatasetLocatorStrategy] = None,
    pipeline_context: Optional[PipelineContextLike] = None,
    return_status: Literal[True],
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
    dataset_locator: Optional[DatasetLocatorStrategy] = None,
    pipeline_context: Optional[PipelineContextLike] = None,
    return_status: Literal[False] = False,
) -> ValidationResult:
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
    dataset_locator: Optional[DatasetLocatorStrategy] = None,
    pipeline_context: Optional[PipelineContextLike] = None,
    return_status: bool = False,
    violation_strategy: Optional[WriteViolationStrategy] = None,
) -> Any:
    """Validate/align a DataFrame then write it using Spark writers.

    Applies the contract schema before writing and merges IO options coming
    from the contract (``io.format``, ``io.write_options``) and user options.
    Returns a ``ValidationResult`` for pre-write checks.
    """
    locator = dataset_locator or ContractFirstDatasetLocator()

    contract: Optional[OpenDataContractStandard] = None
    if contract_id:
        contract = _resolve_contract(
            contract_id=contract_id,
            expected_version=expected_contract_version,
            service=contract_service,
        )
        ensure_version(contract)
        _check_contract_version(expected_contract_version, contract.version)

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
        # validate before write and always align schema for downstream metrics
        expectation_plan = list(
            data_quality_service.describe_expectations(contract=contract)
        )
        observed_schema, observed_metrics = collect_observations(
            df,
            contract,
            expectations=expectation_plan,
        )
        result = _evaluate_with_service(
            contract=contract,
            service=data_quality_service,
            schema=observed_schema,
            metrics=observed_metrics,
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
        if not result.ok:
            if enforce:
                raise ValueError(f"Contract validation failed: {result.errors}")

    options_dict: Dict[str, str] = {}
    if resolution.write_options:
        options_dict.update(resolution.write_options)
    if options:
        options_dict.update(options)
    expectation_predicates: Mapping[str, str] = {}
    predicates = result.details.get("expectation_predicates")
    if isinstance(predicates, Mapping):
        expectation_predicates = dict(predicates)

    strategy = violation_strategy or NoOpWriteViolationStrategy()
    revalidator: Callable[[DataFrame], ValidationResult]
    if contract:

        def revalidator(new_df: DataFrame) -> ValidationResult:  # type: ignore[misc]
            schema, metrics = collect_observations(
                new_df,
                contract,
                expectations=expectation_plan,
            )
            return _evaluate_with_service(
                contract=contract,
                service=data_quality_service,
                schema=schema,
                metrics=metrics,
            )
    else:
        revalidator = lambda new_df: ValidationResult(  # type: ignore[return-value]
            ok=True,
            errors=[],
            warnings=[],
            metrics={},
            schema={},
        )

    base_pipeline_context = normalise_pipeline_context(pipeline_context)

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
        dataset_id=resolution.dataset_id,
        dataset_version=resolution.dataset_version,
        revalidate=revalidator,
        expectation_predicates=expectation_predicates,
        pipeline_context=base_pipeline_context,
    )
    plan = strategy.plan(context)

    requests: list[WriteRequest] = []
    primary_status: Optional[ValidationResult] = None
    validations: list[ValidationResult] = []
    status_records: list[tuple[Optional[ValidationResult], WriteRequest]] = []

    if plan.primary is not None:
        requests.append(plan.primary)

    requests.extend(list(plan.additional))

    for req in requests:
        req.pipeline_context = _merge_pipeline_context(
            base_pipeline_context,
            req.pipeline_context,
        )

    if not requests:
        final_result = plan.result_factory() if plan.result_factory else result
        if return_status:
            return final_result, None
        return final_result

    for index, request in enumerate(requests):
        status, request_validation = _execute_write_request(
            request,
            governance_client=governance_client,
            enforce=enforce,
        )
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
                        if not primary_status.reason:
                            primary_status.reason = (
                                "Violations isolated into auxiliary outputs"
                            )
                        primary_status.status = "warn"

                if merged_warnings:
                    existing_warnings = list(primary_details.get("warnings", []) or [])
                    for warning in merged_warnings:
                        if warning not in existing_warnings:
                            existing_warnings.append(warning)
                    if existing_warnings:
                        primary_details["warnings"] = existing_warnings

                if merged_errors:
                    existing_errors = list(primary_details.get("errors", []) or [])
                    for error in merged_errors:
                        if error not in existing_errors:
                            existing_errors.append(error)
                    if existing_errors:
                        primary_details["errors"] = existing_errors

                if override_note:
                    overrides = list(primary_details.get("overrides", []) or [])
                    overrides.append(override_note)
                    primary_details["overrides"] = overrides
                    if original_status:
                        primary_details.setdefault("status_before_override", original_status)
                    if primary_entry is not None:
                        entry_details = dict(primary_entry.get("details", {}) or {})
                        entry_overrides = list(entry_details.get("overrides", []) or [])
                        if override_note not in entry_overrides:
                            entry_overrides.append(override_note)
                        if original_status:
                            entry_details.setdefault("status_before_override", original_status)
                        if entry_overrides:
                            entry_details["overrides"] = entry_overrides
                        primary_entry["details"] = entry_details
                        primary_entry["status"] = primary_status.status

                primary_status.details = primary_details

    if return_status:
        return final_result, primary_status
    return final_result


def _execute_write_request(
    request: WriteRequest,
    *,
    governance_client: Optional[GovernanceServiceClient],
    enforce: bool,
) -> tuple[Optional[ValidationResult], Optional[ValidationResult]]:
    writer = request.df.write
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

    validation = request.validation_factory() if request.validation_factory else None
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
                request.df.sparkSession,
                table=request.table,
                path=request.path,
            )
            or "unknown"
        )

        def _post_write_observations() -> ObservationPayload:
            metrics, schema_payload, reused_metrics = build_metrics_payload(
                request.df,
                contract,
                validation=validation,
                include_schema=True,
                expectations=expectation_plan,
            )
            if reused_metrics:
                logger.info(
                    "Using cached validation metrics for %s@%s",
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
        if status:
            logger.info(
                "DQ status for %s@%s after write: %s",
                dq_dataset_id,
                dq_dataset_version,
                status.status,
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

    return status, validation
