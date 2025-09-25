from __future__ import annotations

"""Spark/Databricks integration helpers.

High-level wrappers to read/write DataFrames while enforcing ODCS contracts
and coordinating with an external Data Quality client when provided.
"""

from typing import Any, Dict, Optional, Tuple, Literal, Mapping, overload
import logging
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession

from dc43.components.data_quality import (
    DataQualityManager,
    DQClient,
    DQStatus,
    QualityDraftContext,
)
from dc43.components.data_quality.engine import ValidationResult
from dc43.components.data_quality.integration import build_metrics_payload, validate_dataframe
from dc43.components.data_quality.validation import apply_contract
from dc43.odcs import contract_identity, ensure_version
from dc43.versioning import SemVer
from open_data_contract_standard.model import OpenDataContractStandard, Server  # type: ignore


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


# Overloads help type checkers infer the return type based on ``return_status``
# so callers can destructure the tuple without false positives.
@overload
def read_with_contract(
    spark: SparkSession,
    *,
    format: Optional[str] = None,
    path: Optional[str] = None,
    table: Optional[str] = None,
    options: Optional[Dict[str, str]] = None,
    contract: Optional[OpenDataContractStandard] = None,
    enforce: bool = True,
    auto_cast: bool = True,
    dq_client: Optional[DataQualityManager | DQClient] = None,
    expected_contract_version: Optional[str] = None,
    dataset_id: Optional[str] = None,
    dataset_version: Optional[str] = None,
    return_status: Literal[True] = True,
) -> tuple[DataFrame, Optional[DQStatus]]:
    ...


@overload
def read_with_contract(
    spark: SparkSession,
    *,
    format: Optional[str] = None,
    path: Optional[str] = None,
    table: Optional[str] = None,
    options: Optional[Dict[str, str]] = None,
    contract: Optional[OpenDataContractStandard] = None,
    enforce: bool = True,
    auto_cast: bool = True,
    dq_client: Optional[DataQualityManager | DQClient] = None,
    expected_contract_version: Optional[str] = None,
    dataset_id: Optional[str] = None,
    dataset_version: Optional[str] = None,
    return_status: Literal[False],
) -> DataFrame:
    ...


@overload
def read_with_contract(
    spark: SparkSession,
    *,
    format: Optional[str] = None,
    path: Optional[str] = None,
    table: Optional[str] = None,
    options: Optional[Dict[str, str]] = None,
    contract: Optional[OpenDataContractStandard] = None,
    enforce: bool = True,
    auto_cast: bool = True,
    dq_client: Optional[DataQualityManager | DQClient] = None,
    expected_contract_version: Optional[str] = None,
    dataset_id: Optional[str] = None,
    dataset_version: Optional[str] = None,
    return_status: bool = True,
) -> DataFrame | tuple[DataFrame, Optional[DQStatus]]:
    ...


def read_with_contract(
    spark: SparkSession,
    *,
    format: Optional[str] = None,
    path: Optional[str] = None,
    table: Optional[str] = None,
    options: Optional[Dict[str, str]] = None,
    contract: Optional[OpenDataContractStandard] = None,
    enforce: bool = True,
    auto_cast: bool = True,
    # Governance / DQ orchestration
    dq_client: Optional[DataQualityManager | DQClient] = None,
    expected_contract_version: Optional[str] = None,  # e.g. '==1.2.0' or '>=1.0.0'
    dataset_id: Optional[str] = None,
    dataset_version: Optional[str] = None,
    return_status: bool = True,
) -> DataFrame | Tuple[DataFrame, Optional[DQStatus]]:
    """Read a DataFrame and validate/enforce an ODCS contract.

    - If ``contract`` is provided, validates schema and aligns columns/types.
    - If ``dq_client`` is provided (either a :class:`DataQualityManager` or a
      raw :class:`DQClient` implementation), checks dataset status and submits
      metrics when needed; returns status when ``return_status=True``.
    """
    # Resolve the physical location from the contract when one is provided.
    #
    # ``read_with_contract`` originally only looked up the path/table when both
    # arguments were omitted.  When tests started to rely solely on the server
    # information contained in the contract document, this behaviour caused the
    # reader to attempt loading an empty path (Spark then warns that *all paths
    # were ignored*).  By always considering the contract's first server we make
    # the function robust regardless of how the caller specifies the location.
    c_fmt: Optional[str] = None
    if contract:
        c_path, c_table = _ref_from_contract(contract)
        c_fmt = contract.servers[0].format if contract.servers else None
        path = path or c_path
        table = table or c_table
        if path and c_path and not _paths_compatible(path, c_path):
            logger.warning(
                "Provided path %s does not match contract server path %s", path, c_path
            )
    if not path and not table:
        raise ValueError("Either table or path must be provided for read")
    if format and c_fmt and format != c_fmt:
        logger.warning(
            "Provided format %s does not match contract server format %s", format, c_fmt
        )
    format = format or c_fmt
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
        ensure_version(contract)
        cid, cver = contract_identity(contract)
        logger.info("Reading with contract %s:%s", cid, cver)
        _check_contract_version(expected_contract_version, cver)
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
        ds_id = dataset_id or dataset_id_from_ref(table=table, path=path)
        ds_ver = dataset_version or get_delta_version(spark, table=table, path=path) or "unknown"

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
            if enforce and status.status == "block":
                raise ValueError(f"DQ status is blocking: {status.reason or status.details}")

    return (df, status) if return_status else df


# Overloads allow static checkers to track the tuple return when ``return_status``
# is requested, avoiding "DataFrame is not iterable" warnings.
@overload
def write_with_contract(
    *,
    df: DataFrame,
    contract: Optional[OpenDataContractStandard] = None,
    path: Optional[str] = None,
    table: Optional[str] = None,
    format: Optional[str] = None,
    options: Optional[Dict[str, str]] = None,
    mode: str = "append",
    enforce: bool = True,
    auto_cast: bool = True,
    dq_client: Optional[DataQualityManager | DQClient] = None,
    dataset_id: Optional[str] = None,
    dataset_version: Optional[str] = None,
    return_status: Literal[True],
) -> tuple[ValidationResult, Optional[DQStatus]]:
    ...


@overload
def write_with_contract(
    *,
    df: DataFrame,
    contract: Optional[OpenDataContractStandard] = None,
    path: Optional[str] = None,
    table: Optional[str] = None,
    format: Optional[str] = None,
    options: Optional[Dict[str, str]] = None,
    mode: str = "append",
    enforce: bool = True,
    auto_cast: bool = True,
    dq_client: Optional[DataQualityManager | DQClient] = None,
    dataset_id: Optional[str] = None,
    dataset_version: Optional[str] = None,
    return_status: Literal[False] = False,
) -> ValidationResult:
    ...


def write_with_contract(
    *,
    df: DataFrame,
    contract: Optional[OpenDataContractStandard] = None,
    path: Optional[str] = None,
    table: Optional[str] = None,
    format: Optional[str] = None,
    options: Optional[Dict[str, str]] = None,
    mode: str = "append",
    enforce: bool = True,
    auto_cast: bool = True,
    dq_client: Optional[DataQualityManager | DQClient] = None,
    dataset_id: Optional[str] = None,
    dataset_version: Optional[str] = None,
    return_status: bool = False,
) -> Any:
    """Validate/align a DataFrame then write it using Spark writers.

    Applies the contract schema before writing and merges IO options coming
    from the contract (``io.format``, ``io.write_options``) and user options.
    Returns a ``ValidationResult`` for pre-write checks.
    """
    # As with ``read_with_contract`` above, always derive the target path or
    # table from the contract when one is supplied.  This allows callers to rely
    # solely on the contract's server definition.
    c_fmt: Optional[str] = None
    if contract:
        c_path, c_table = _ref_from_contract(contract)
        c_fmt = contract.servers[0].format if contract.servers else None
        path = path or c_path
        table = table or c_table

    out_df = df
    quality_manager = _as_quality_manager(dq_client)
    result = ValidationResult(ok=True, errors=[], warnings=[], metrics={})
    if contract:
        ensure_version(contract)
        cid, cver = contract_identity(contract)
        logger.info("Writing with contract %s:%s", cid, cver)
        # validate before write and always align schema for downstream metrics
        result = validate_dataframe(df, contract)
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
        format = format or c_fmt
        if path and c_path and not _paths_compatible(path, c_path):
            msg = f"Path {path} does not match contract server path {c_path}"
            logger.warning(msg)
            result.warnings.append(msg)
        if not result.ok:
            if enforce:
                raise ValueError(f"Contract validation failed: {result.errors}")

    writer = out_df.write
    if format:
        writer = writer.format(format)
    if options:
        writer = writer.options(**options)
    writer = writer.mode(mode)
    if table:
        logger.info("Writing dataframe to table %s", table)
        writer.saveAsTable(table)
    else:
        if not path:
            raise ValueError("Either table or path must be provided for write")
        logger.info("Writing dataframe to path %s", path)
        writer.save(path)

    # DQ integration after write
    status: Optional[DQStatus] = None
    if quality_manager and contract:
        dq_dataset_id = dataset_id or dataset_id_from_ref(table=table, path=path)
        dq_dataset_version = (
            dataset_version
            or get_delta_version(df.sparkSession, table=table, path=path)
            or "unknown"
        )

        def _post_write_observations() -> tuple[Mapping[str, Any], bool]:
            metrics, _schema_payload, reused_metrics = build_metrics_payload(
                out_df,
                contract,
                validation=result,
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
            validation=result,
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
        if contract:
            if not result.ok:
                request_draft = True
            elif status and status.status not in (None, "ok"):
                request_draft = True
        if request_draft:
            dq_feedback: Mapping[str, Any] | None = None
            if status and status.details:
                dq_feedback = dict(status.details)
                if status.reason:
                    dq_feedback.setdefault("reason", status.reason)
                dq_feedback.setdefault("status", status.status)
            context = QualityDraftContext(
                dataset_id=dq_dataset_id,
                dataset_version=dq_dataset_version,
                data_format=format,
                dq_feedback=dq_feedback,
            )
            draft_contract = quality_manager.review_validation_outcome(
                validation=result,
                base_contract=contract,
                context=context,
                draft_requested=True,
            )
            if draft_contract is not None:
                if status:
                    details = dict(status.details or {})
                    details.setdefault("draft_contract_version", draft_contract.version)
                    status.details = details
        if assessment.draft and enforce:
            raise ValueError(
                "DQ governance returned a draft contract for the submitted dataset, "
                "indicating the provided contract version is out of date",
            )

    if return_status:
        return result, status
    return result
