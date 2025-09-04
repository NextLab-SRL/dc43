from __future__ import annotations

"""Spark/Databricks integration helpers.

High-level wrappers to read/write DataFrames while enforcing ODCS contracts
and coordinating with an external Data Quality client when provided.
"""

from typing import Any, Dict, Optional, Tuple, Literal, overload
import logging
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession

from .validation import validate_dataframe, apply_contract, ValidationResult
from ..dq.base import DQClient, DQStatus
from ..dq.spark_metrics import compute_metrics
from .dataset import get_delta_version, dataset_id_from_ref
from ..versioning import SemVer
from ..odcs import contract_identity, ensure_version
from open_data_contract_standard.model import (
    SchemaProperty,
    SchemaObject,
    CustomProperty,
    OpenDataContractStandard,
    Server,
)  # type: ignore
from ..storage.base import ContractStore

logger = logging.getLogger(__name__)


def _simple_contract_id(dataset_id: str) -> str:
    """Return a human-friendly contract id from a dataset reference."""
    from pathlib import Path

    if dataset_id.startswith("path:"):
        p = Path(dataset_id[5:])
        # Use the parent directory name, dropping version segments
        return p.parent.name or p.name
    if dataset_id.startswith("table:"):
        return dataset_id.split(":", 1)[1]
    return dataset_id

def _propose_draft_from_dataframe(
    df: DataFrame,
    contract_doc: OpenDataContractStandard,
    *,
    bump: str = "minor",
    dataset_id: Optional[str] = None,
    dataset_version: Optional[str] = None,
    data_format: Optional[str] = None,
) -> OpenDataContractStandard:
    """Create a draft ODCS doc based on the DataFrame schema and base contract.

    - Copies io/expectations/metadata from base, bumps version, replaces fields
    - Adds metadata.draft=true and provenance info
    """
    from .validation import SPARK_TYPES as _SPARK_TYPES  # reuse mapping
    from pyspark.sql import functions as F

    # Build new field list from df schema
    props = []
    for f in df.schema.fields:  # type: ignore[attr-defined]
        t = str(f.dataType).lower()
        # Inverse mapping: crude normalization of Spark dtype string
        # Heuristic: find key whose spark type matches suffix in dtype string
        odcs_type = None
        for k, v in _SPARK_TYPES.items():
            if v in t:
                odcs_type = k
                break
        odcs_type = odcs_type or t
        props.append(
            SchemaProperty(
                name=f.name,
                physicalType=str(odcs_type),
                required=not f.nullable,
            )
        )

    # Bump version
    cid, ver = contract_identity(contract_doc)
    sv = SemVer.parse(ver)
    nver = str(sv.bump("minor" if bump not in ("major", "patch") else bump))

    # Preserve existing customProperties and append draft metadata
    cps = list(contract_doc.customProperties or [])
    cps.append(CustomProperty(property="draft", value=True))
    cps.append(CustomProperty(property="base_version", value=ver))
    cps.append(CustomProperty(property="provenance", value={"dataset_id": dataset_id, "dataset_version": dataset_version}))

    schema_name = cid
    if contract_doc.schema_:
        first = contract_doc.schema_[0]
        schema_name = first.name or cid

    servers = contract_doc.servers
    if dataset_id:
        base_fmt = data_format
        if not base_fmt and contract_doc.servers:
            base_fmt = contract_doc.servers[0].format
        if dataset_id.startswith("path:"):
            servers = [
                Server(server="local", type="filesystem", path=dataset_id[5:], format=base_fmt)
            ]
        elif dataset_id.startswith("table:"):
            servers = [Server(server="local", dataset=dataset_id[6:], format=base_fmt)]

    draft = OpenDataContractStandard(
        version=nver,
        kind=contract_doc.kind,
        apiVersion=contract_doc.apiVersion,
        id=cid,
        name=contract_doc.name or cid,
        description=contract_doc.description,
        status="draft",
        schema=[SchemaObject(name=schema_name, properties=props)],
        servers=servers,
        customProperties=cps,
    )
    return draft


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
    dq_client: Optional[DQClient] = None,
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
    dq_client: Optional[DQClient] = None,
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
    dq_client: Optional[DQClient] = None,
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
    dq_client: Optional[DQClient] = None,
    expected_contract_version: Optional[str] = None,  # e.g. '==1.2.0' or '>=1.0.0'
    dataset_id: Optional[str] = None,
    dataset_version: Optional[str] = None,
    return_status: bool = True,
) -> DataFrame | Tuple[DataFrame, Optional[DQStatus]]:
    """Read a DataFrame and validate/enforce an ODCS contract.

    - If ``contract`` is provided, validates schema and aligns columns/types.
    - If ``dq_client`` is provided, checks dataset status and submits metrics
      when needed; returns status when ``return_status=True``.
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
        if path and c_path and Path(path).resolve() != Path(c_path).resolve():
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
    status: Optional[DQStatus] = None
    if dq_client and contract:
        ds_id = dataset_id or dataset_id_from_ref(table=table, path=path)
        ds_ver = dataset_version or get_delta_version(spark, table=table, path=path) or "unknown"

        # Check dataset->contract linkage if tracked; link when missing
        linked = dq_client.get_linked_contract_version(dataset_id=ds_id)
        if linked and linked != f"{cid}:{cver}":
            status = DQStatus(status="block", reason=f"dataset linked to {linked}")
        else:
            if not linked:
                dq_client.link_dataset_contract(
                    dataset_id=ds_id,
                    dataset_version=ds_ver,
                    contract_id=cid,
                    contract_version=cver,
                )
            status = dq_client.get_status(
                contract_id=cid,
                contract_version=cver,
                dataset_id=ds_id,
                dataset_version=ds_ver,
            )
            if status.status in ("unknown", "stale"):
                m = compute_metrics(df, contract)
                logger.info("Computing DQ metrics for %s@%s", ds_id, ds_ver)
                status = dq_client.submit_metrics(
                    contract=contract, dataset_id=ds_id, dataset_version=ds_ver, metrics=m
                )
        logger.info("DQ status for %s@%s: %s", ds_id, ds_ver, status.status)
        if enforce and status and status.status == "block":
            raise ValueError(f"DQ status is blocking: {status.reason or status.details}")

    return (df, status) if return_status else df


# Overloads allow static checkers to track the tuple return when combinations of
# ``return_draft`` and ``return_status`` are requested, avoiding "DataFrame is
# not iterable" warnings.
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
    draft_on_mismatch: bool = False,
    draft_store: Optional[ContractStore] = None,
    draft_bump: str = "minor",
    dq_client: Optional[DQClient] = None,
    dataset_id: Optional[str] = None,
    dataset_version: Optional[str] = None,
    return_status: Literal[True],
    return_draft: Literal[True],
) -> tuple[ValidationResult, Optional[DQStatus], Optional[OpenDataContractStandard]]:
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
    draft_on_mismatch: bool = False,
    draft_store: Optional[ContractStore] = None,
    draft_bump: str = "minor",
    dq_client: Optional[DQClient] = None,
    dataset_id: Optional[str] = None,
    dataset_version: Optional[str] = None,
    return_status: Literal[True],
    return_draft: Literal[False],
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
    draft_on_mismatch: bool = False,
    draft_store: Optional[ContractStore] = None,
    draft_bump: str = "minor",
    dq_client: Optional[DQClient] = None,
    dataset_id: Optional[str] = None,
    dataset_version: Optional[str] = None,
    return_status: Literal[False] = False,
    return_draft: Literal[True] = True,
) -> tuple[ValidationResult, Optional[OpenDataContractStandard]]:
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
    draft_on_mismatch: bool = False,
    draft_store: Optional[ContractStore] = None,
    draft_bump: str = "minor",
    dq_client: Optional[DQClient] = None,
    dataset_id: Optional[str] = None,
    dataset_version: Optional[str] = None,
    return_status: Literal[False] = False,
    return_draft: Literal[False] = False,
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
    # Draft flow on mismatch
    draft_on_mismatch: bool = False,
    draft_store: Optional[ContractStore] = None,
    draft_bump: str = "minor",
    # DQ integration
    dq_client: Optional[DQClient] = None,
    dataset_id: Optional[str] = None,
    dataset_version: Optional[str] = None,
    return_status: bool = False,
    return_draft: bool = True,
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
    draft_doc: Optional[OpenDataContractStandard] = None
    # Default to an "all good" validation result; this will be replaced when a
    # contract is actually enforced below.
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
            if draft_on_mismatch and draft_doc is None:
                ds_id = dataset_id_from_ref(table=table, path=path)
                ds_ver = (
                    get_delta_version(df.sparkSession, table=table, path=path)
                    if hasattr(df, "sparkSession")
                    else None
                )
                draft_doc = _propose_draft_from_dataframe(
                    df,
                    contract,
                    bump=draft_bump,
                    dataset_id=ds_id,
                    dataset_version=ds_ver,
                    data_format=format,
                )
                if draft_store is not None:
                    logger.info(
                        "Persisting draft contract %s:%s due to format mismatch",
                        draft_doc.id,
                        draft_doc.version,
                    )
                    draft_store.put(draft_doc)
        format = format or c_fmt
        if path and c_path and Path(path).resolve() != Path(c_path).resolve():
            msg = f"Path {path} does not match contract server path {c_path}"
            logger.warning(msg)
            result.warnings.append(msg)
            if draft_on_mismatch and draft_doc is None:
                ds_id = dataset_id_from_ref(table=table, path=path)
                ds_ver = (
                    get_delta_version(df.sparkSession, table=table, path=path)
                    if hasattr(df, "sparkSession")
                    else None
                )
                draft_doc = _propose_draft_from_dataframe(
                    df,
                    contract,
                    bump=draft_bump,
                    dataset_id=ds_id,
                    dataset_version=ds_ver,
                    data_format=format,
                )
                if draft_store is not None:
                    logger.info(
                        "Persisting draft contract %s:%s due to path mismatch",
                        draft_doc.id,
                        draft_doc.version,
                    )
                    draft_store.put(draft_doc)
        if not result.ok:
            if draft_on_mismatch and draft_doc is None:
                ds_id = dataset_id_from_ref(table=table, path=path) if (table or path) else "unknown"
                ds_ver = (
                    get_delta_version(df.sparkSession, table=table, path=path)
                    if hasattr(df, "sparkSession")
                    else None
                )
                draft_doc = _propose_draft_from_dataframe(
                    df,
                    contract,
                    bump=draft_bump,
                    dataset_id=ds_id,
                    dataset_version=ds_ver,
                    data_format=format,
                )
                if draft_store is not None:
                    logger.info(
                        "Persisting draft contract %s:%s due to mismatch",
                        draft_doc.id,
                        draft_doc.version,
                    )
                    draft_store.put(draft_doc)
            if enforce:
                raise ValueError(f"Contract validation failed: {result.errors}")
    elif draft_store is not None:
        # No contract supplied: infer one from the DataFrame schema and persist as
        # a draft so callers can review or evolve it later.
        ds_id_raw = dataset_id_from_ref(table=table, path=path) if (table or path) else "unknown"
        ds_id = _simple_contract_id(ds_id_raw)
        ds_ver = (
            get_delta_version(df.sparkSession, table=table, path=path)
            if hasattr(df, "sparkSession")
            else None
        )
        base = OpenDataContractStandard(
            version="0.0.0",
            kind="DataContract",
            apiVersion="3.0.2",
            id=ds_id,
            name=ds_id,
        )
        draft_doc = _propose_draft_from_dataframe(
            df,
            base,
            bump=draft_bump,
            dataset_id=ds_id_raw,
            dataset_version=ds_ver,
            data_format=format,
        )
        logger.info(
            "Persisting inferred draft contract %s:%s",
            draft_doc.id,
            draft_doc.version,
        )
        draft_store.put(draft_doc)

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
    if dq_client and contract:
        ds_id = dataset_id or dataset_id_from_ref(table=table, path=path)
        ds_ver = dataset_version or get_delta_version(df.sparkSession, table=table, path=path) or "unknown"
        linked = dq_client.get_linked_contract_version(dataset_id=ds_id)
        if linked and linked != f"{cid}:{cver}":
            status = DQStatus(status="block", reason=f"dataset linked to {linked}")
        else:
            if not linked:
                dq_client.link_dataset_contract(
                    dataset_id=ds_id,
                    dataset_version=ds_ver,
                    contract_id=cid,
                    contract_version=cver,
                )
            metrics = compute_metrics(out_df, contract)
            status = dq_client.submit_metrics(
                contract=contract, dataset_id=ds_id, dataset_version=ds_ver, metrics=metrics
            )
        if enforce and status and status.status == "block":
            raise ValueError(f"DQ violation: {status.details}")

    # Propagate the validation result to callers.
    if return_status and return_draft:
        return result, status, draft_doc
    if return_status:
        return result, status
    if return_draft:
        return result, draft_doc
    return result
