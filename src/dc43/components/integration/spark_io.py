from __future__ import annotations

"""Spark/Databricks integration helpers.

High-level wrappers to read/write DataFrames while enforcing ODCS contracts
and coordinating with an external Data Quality client when provided.
"""

from typing import (
    Any,
    Callable,
    Dict,
    Mapping,
    Optional,
    Protocol,
    Tuple,
    Literal,
    overload,
)
import logging
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession

from dc43.components.contract_store.interface import ContractStore
from dc43.components.data_quality import DataQualityManager, DQClient, DQStatus
from dc43.components.data_quality.engine import ValidationResult
from dc43.components.data_quality.integration import (
    build_metrics_payload,
    expectations_from_contract,
    validate_dataframe,
)
from dc43.components.data_quality.validation import apply_contract
from dc43.odcs import contract_identity, ensure_version
from dc43.versioning import SemVer
from open_data_contract_standard.model import OpenDataContractStandard, Server  # type: ignore

from .violation_strategy import (
    NoOpWriteViolationStrategy,
    WriteRequest,
    WriteStrategyContext,
    WriteViolationStrategy,
)


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


def dataset_id_from_ref(*, table: Optional[str] = None, path: Optional[str] = None) -> str:
    """Build a dataset id from a table name or path (``table:...``/``path:...``)."""

    if table:
        return f"table:{table}"
    if path:
        return f"path:{path}"
    return "unknown"

logger = logging.getLogger(__name__)


def _as_quality_manager(dq: Optional[DataQualityManager | DQClient]) -> Optional[DataQualityManager]:
    """Return a :class:`DataQualityManager` regardless of input flavour."""

    if dq is None:
        return None
    if isinstance(dq, DataQualityManager):
        return dq
    return DataQualityManager(dq)


@dataclass
class DatasetResolution:
    """Resolved location and governance identifiers for a dataset."""

    path: Optional[str]
    table: Optional[str]
    format: Optional[str]
    dataset_id: Optional[str]
    dataset_version: Optional[str]


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
    ) -> tuple[Optional[str], Optional[str], Optional[str]]:
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
        return path, table, format

    def _resolution(
        self,
        *,
        path: Optional[str],
        table: Optional[str],
        format: Optional[str],
    ) -> DatasetResolution:
        dataset_id = dataset_id_from_ref(table=table, path=path)
        dataset_version = self.clock()
        return DatasetResolution(
            path=path,
            table=table,
            format=format,
            dataset_id=dataset_id,
            dataset_version=dataset_version,
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
        path, table, format = self._resolve_base(contract, path=path, table=table, format=format)
        return self._resolution(path=path, table=table, format=format)

    def for_write(
        self,
        *,
        contract: Optional[OpenDataContractStandard],
        df: DataFrame,
        format: Optional[str],
        path: Optional[str],
        table: Optional[str],
    ) -> DatasetResolution:  # noqa: D401 - short docstring
        path, table, format = self._resolve_base(contract, path=path, table=table, format=format)
        return self._resolution(path=path, table=table, format=format)


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
        status: Optional[DQStatus],
        enforce: bool,
        context: ReadStatusContext,
    ) -> tuple[DataFrame, Optional[DQStatus]]:
        ...


@dataclass
class DefaultReadStatusStrategy:
    """Default behaviour preserving enforcement semantics."""

    def apply(
        self,
        *,
        dataframe: DataFrame,
        status: Optional[DQStatus],
        enforce: bool,
        context: ReadStatusContext,
    ) -> tuple[DataFrame, Optional[DQStatus]]:  # noqa: D401 - short docstring
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
    store: ContractStore,
) -> OpenDataContractStandard:
    """Fetch a contract from ``store`` honouring the expected version constraint."""

    if store is None:
        raise ValueError("contract_store is required when contract_id is provided")

    if not expected_version:
        contract = store.latest(contract_id)
        if contract is None:
            raise ValueError(f"No versions available for contract {contract_id}")
        return contract

    if expected_version.startswith("=="):
        version = expected_version[2:]
        return store.get(contract_id, version)

    if expected_version.startswith(">="):
        base = expected_version[2:]
        version = _select_version(store.list_versions(contract_id), base)
        return store.get(contract_id, version)

    return store.get(contract_id, expected_version)


# Overloads help type checkers infer the return type based on ``return_status``
# so callers can destructure the tuple without false positives.
@overload
def read_with_contract(
    spark: SparkSession,
    *,
    contract_id: Optional[str] = None,
    contract_store: Optional[ContractStore] = None,
    expected_contract_version: Optional[str] = None,
    format: Optional[str] = None,
    path: Optional[str] = None,
    table: Optional[str] = None,
    options: Optional[Dict[str, str]] = None,
    enforce: bool = True,
    auto_cast: bool = True,
    dq_client: Optional[DataQualityManager | DQClient] = None,
    dataset_locator: Optional[DatasetLocatorStrategy] = None,
    status_strategy: Optional[ReadStatusStrategy] = None,
    return_status: Literal[True] = True,
) -> tuple[DataFrame, Optional[DQStatus]]:
    ...


@overload
def read_with_contract(
    spark: SparkSession,
    *,
    contract_id: Optional[str] = None,
    contract_store: Optional[ContractStore] = None,
    expected_contract_version: Optional[str] = None,
    format: Optional[str] = None,
    path: Optional[str] = None,
    table: Optional[str] = None,
    options: Optional[Dict[str, str]] = None,
    enforce: bool = True,
    auto_cast: bool = True,
    dq_client: Optional[DataQualityManager | DQClient] = None,
    dataset_locator: Optional[DatasetLocatorStrategy] = None,
    status_strategy: Optional[ReadStatusStrategy] = None,
    return_status: Literal[False],
) -> DataFrame:
    ...


@overload
def read_with_contract(
    spark: SparkSession,
    *,
    contract_id: Optional[str] = None,
    contract_store: Optional[ContractStore] = None,
    expected_contract_version: Optional[str] = None,
    format: Optional[str] = None,
    path: Optional[str] = None,
    table: Optional[str] = None,
    options: Optional[Dict[str, str]] = None,
    enforce: bool = True,
    auto_cast: bool = True,
    dq_client: Optional[DataQualityManager | DQClient] = None,
    dataset_locator: Optional[DatasetLocatorStrategy] = None,
    status_strategy: Optional[ReadStatusStrategy] = None,
    return_status: bool = True,
) -> DataFrame | tuple[DataFrame, Optional[DQStatus]]:
    ...


def read_with_contract(
    spark: SparkSession,
    *,
    contract_id: Optional[str] = None,
    contract_store: Optional[ContractStore] = None,
    expected_contract_version: Optional[str] = None,
    format: Optional[str] = None,
    path: Optional[str] = None,
    table: Optional[str] = None,
    options: Optional[Dict[str, str]] = None,
    enforce: bool = True,
    auto_cast: bool = True,
    # Governance / DQ orchestration
    dq_client: Optional[DataQualityManager | DQClient] = None,
    dataset_locator: Optional[DatasetLocatorStrategy] = None,
    status_strategy: Optional[ReadStatusStrategy] = None,
    return_status: bool = True,
) -> DataFrame | Tuple[DataFrame, Optional[DQStatus]]:
    """Read a DataFrame and validate/enforce an ODCS contract.

    - If ``contract_id`` is provided, the contract is fetched from ``contract_store``
      before validating schema and aligning columns/types.
    - If ``dq_client`` is provided (either a :class:`DataQualityManager` or a
      raw :class:`DQClient` implementation), checks dataset status and submits
      metrics when needed; returns status when ``return_status=True``.
    """
    locator = dataset_locator or ContractFirstDatasetLocator()
    status_handler = status_strategy or DefaultReadStatusStrategy()

    contract: Optional[OpenDataContractStandard] = None
    if contract_id:
        contract = _resolve_contract(
            contract_id=contract_id,
            expected_version=expected_contract_version,
            store=contract_store,
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

    if not path and not table:
        raise ValueError("Either table or path must be provided for read")

    reader = spark.read
    if format:
        reader = reader.format(format)
    if options:
        reader = reader.options(**options)
    df = reader.table(table) if table else reader.load(path)
    result: Optional[ValidationResult] = None
    cid: Optional[str] = None
    cver: Optional[str] = None
    if contract:
        cid, cver = contract_identity(contract)
        logger.info("Reading with contract %s:%s", cid, cver)
        result = validate_dataframe(df, contract)
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
    quality_manager = _as_quality_manager(dq_client)
    status: Optional[DQStatus] = None
    if quality_manager and contract and result is not None:
        ds_id = resolution.dataset_id or dataset_id_from_ref(table=table, path=path)
        ds_ver = (
            resolution.dataset_version
            or get_delta_version(spark, table=table, path=path)
            or "unknown"
        )

        def _observations() -> tuple[Mapping[str, object], bool]:
            metrics_payload, _schema_payload, reused = build_metrics_payload(
                df,
                contract,
                validation=result,
                include_schema=True,
            )
            if reused:
                logger.info("Using cached validation metrics for %s@%s", ds_id, ds_ver)
            else:
                logger.info("Computing DQ metrics for %s@%s", ds_id, ds_ver)
            return metrics_payload, reused

        assessment = quality_manager.evaluate_dataset(
            contract=contract,
            dataset_id=ds_id,
            dataset_version=ds_ver,
            validation=result,
            observations=_observations,
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
    contract_store: Optional[ContractStore] = None,
    expected_contract_version: Optional[str] = None,
    path: Optional[str] = None,
    table: Optional[str] = None,
    format: Optional[str] = None,
    options: Optional[Dict[str, str]] = None,
    mode: str = "append",
    enforce: bool = True,
    auto_cast: bool = True,
    dq_client: Optional[DataQualityManager | DQClient] = None,
    dataset_locator: Optional[DatasetLocatorStrategy] = None,
    return_status: Literal[True],
) -> tuple[ValidationResult, Optional[DQStatus]]:
    ...


@overload
def write_with_contract(
    *,
    df: DataFrame,
    contract_id: Optional[str] = None,
    contract_store: Optional[ContractStore] = None,
    expected_contract_version: Optional[str] = None,
    path: Optional[str] = None,
    table: Optional[str] = None,
    format: Optional[str] = None,
    options: Optional[Dict[str, str]] = None,
    mode: str = "append",
    enforce: bool = True,
    auto_cast: bool = True,
    dq_client: Optional[DataQualityManager | DQClient] = None,
    dataset_locator: Optional[DatasetLocatorStrategy] = None,
    return_status: Literal[False] = False,
) -> ValidationResult:
    ...


def write_with_contract(
    *,
    df: DataFrame,
    contract_id: Optional[str] = None,
    contract_store: Optional[ContractStore] = None,
    expected_contract_version: Optional[str] = None,
    path: Optional[str] = None,
    table: Optional[str] = None,
    format: Optional[str] = None,
    options: Optional[Dict[str, str]] = None,
    mode: str = "append",
    enforce: bool = True,
    auto_cast: bool = True,
    dq_client: Optional[DataQualityManager | DQClient] = None,
    dataset_locator: Optional[DatasetLocatorStrategy] = None,
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
            store=contract_store,
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
    quality_manager = _as_quality_manager(dq_client)
    result = ValidationResult(ok=True, errors=[], warnings=[], metrics={})
    if contract:
        cid, cver = contract_identity(contract)
        logger.info("Writing with contract %s:%s", cid, cver)
        # validate before write and always align schema for downstream metrics
        result = validate_dataframe(df, contract)
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

    options_dict = dict(options) if options else {}
    expectation_map: Mapping[str, str] = expectations_from_contract(contract) if contract else {}

    strategy = violation_strategy or NoOpWriteViolationStrategy()
    revalidator: Callable[[DataFrame], ValidationResult]
    if contract:
        revalidator = lambda new_df: validate_dataframe(new_df, contract)  # type: ignore[misc]
    else:
        revalidator = lambda new_df: ValidationResult(  # type: ignore[return-value]
            ok=True,
            errors=[],
            warnings=[],
            metrics={},
            schema={},
        )

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
        expectation_predicates=expectation_map,
    )
    plan = strategy.plan(context)

    requests: list[WriteRequest] = []
    primary_status: Optional[DQStatus] = None
    validations: list[ValidationResult] = []
    status_records: list[tuple[Optional[DQStatus], WriteRequest]] = []

    if plan.primary is not None:
        requests.append(plan.primary)

    requests.extend(list(plan.additional))

    if not requests:
        final_result = plan.result_factory() if plan.result_factory else result
        if return_status:
            return final_result, None
        return final_result

    for index, request in enumerate(requests):
        status, request_validation = _execute_write_request(
            request,
            quality_manager=quality_manager,
            enforce=enforce,
        )
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
                if aggregated_violations:
                    primary_details["violations"] = aggregated_violations
                if aggregated_draft and not primary_details.get("draft_contract_version"):
                    primary_details["draft_contract_version"] = aggregated_draft

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

                primary_status.details = primary_details

    if return_status:
        return final_result, primary_status
    return final_result


def _execute_write_request(
    request: WriteRequest,
    *,
    quality_manager: Optional[DataQualityManager],
    enforce: bool,
) -> tuple[Optional[DQStatus], Optional[ValidationResult]]:
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
    if validation is not None and request.warnings:
        for message in request.warnings:
            if message not in validation.warnings:
                validation.warnings.append(message)
    contract = request.contract
    status: Optional[DQStatus] = None
    if quality_manager and contract and validation is not None:
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

        def _post_write_observations() -> tuple[Mapping[str, Any], bool]:
            metrics, _schema_payload, reused_metrics = build_metrics_payload(
                request.df,
                contract,
                validation=validation,
                include_schema=True,
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
            return metrics, reused_metrics

        assessment = quality_manager.evaluate_dataset(
            contract=contract,
            dataset_id=dq_dataset_id,
            dataset_version=dq_dataset_version,
            validation=validation,
            observations=_post_write_observations,
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
            draft_contract = quality_manager.review_validation_outcome(
                validation=validation,
                base_contract=contract,
                dataset_id=dq_dataset_id,
                dataset_version=dq_dataset_version,
                data_format=request.format,
                dq_status=status,
                draft_requested=True,
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

    return status, validation
