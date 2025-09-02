from __future__ import annotations

"""Spark/Databricks integration helpers.

High-level wrappers to read/write DataFrames while enforcing ODCS contracts
and coordinating with an external Data Quality client when provided.
"""

from typing import Any, Dict, Optional, Tuple

try:
    from pyspark.sql import DataFrame, SparkSession
except Exception:  # pragma: no cover
    SparkSession = Any  # type: ignore
    DataFrame = Any  # type: ignore

from .validation import validate_dataframe, apply_contract, ValidationResult
from ..dq.base import DQClient, DQStatus
from ..dq.metrics import compute_metrics
from .dataset import get_delta_version, dataset_id_from_ref
from ..versioning import SemVer
from ..odcs import contract_identity, ensure_version
from open_data_contract_standard.model import SchemaProperty, SchemaObject, CustomProperty  # type: ignore
from open_data_contract_standard.model import OpenDataContractStandard  # type: ignore
from ..storage.base import ContractStore

def _propose_draft_from_dataframe(
    df: DataFrame,
    contract_doc: OpenDataContractStandard,
    *,
    bump: str = "minor",
    dataset_id: Optional[str] = None,
    dataset_version: Optional[str] = None,
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

    draft = OpenDataContractStandard(
        version=nver,
        kind=contract_doc.kind,
        apiVersion=contract_doc.apiVersion,
        id=cid,
        name=contract_doc.name or cid,
        description=contract_doc.description,
        status="draft",
        schema=[SchemaObject(name=schema_name, properties=props)],
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


def read_with_contract(
    spark: SparkSession,
    *,
    format: str,
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
    return_status: bool = False,
) -> DataFrame:
    """Read a DataFrame and validate/enforce an ODCS contract.

    - If ``contract`` is provided, validates schema and aligns columns/types.
    - If ``dq_client`` is provided, checks dataset status and submits metrics
      when needed; returns status when ``return_status=True``.
    """
    reader = spark.read.format(format)
    if options:
        reader = reader.options(**options)
    df = reader.table(table) if table else reader.load(path)
    if contract:
        ensure_version(contract)
        cid, cver = contract_identity(contract)
        _check_contract_version(expected_contract_version, cver)
        result = validate_dataframe(df, contract)
        if not result.ok and enforce:
            raise ValueError(f"Contract validation failed: {result.errors}")
        df = apply_contract(df, contract, auto_cast=auto_cast)

    # DQ integration
    status: Optional[DQStatus] = None
    if dq_client and contract:
        ds_id = dataset_id or dataset_id_from_ref(table=table, path=path)
        ds_ver = dataset_version or get_delta_version(spark, table=table, path=path) or "unknown"

        # Check dataset->contract linkage if tracked
        linked = dq_client.get_linked_contract_version(dataset_id=ds_id)
        if linked and linked != f"{cid}:{cver}":
            status = DQStatus(status="block", reason=f"dataset linked to {linked}")
        else:
            status = dq_client.get_status(
                contract_id=cid,
                contract_version=cver,
                dataset_id=ds_id,
                dataset_version=ds_ver,
            )
            if status.status in ("unknown", "stale"):
                spec = dq_client.expected_metrics(contract)
                m = compute_metrics(df, contract, spec)
                status = dq_client.submit_metrics(
                    contract=contract, dataset_id=ds_id, dataset_version=ds_ver, metrics=m
                )
        if enforce and status and status.status == "block":
            raise ValueError(f"DQ status is blocking: {status.reason or status.details}")

    return (df, status) if return_status else df


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
    return_draft: bool = False,
) -> ValidationResult | Tuple[ValidationResult, Optional[OpenDataContractStandard]]:
    """Validate/align a DataFrame then write it using Spark writers.

    Applies the contract schema before writing and merges IO options coming
    from the contract (``io.format``, ``io.write_options``) and user options.
    Returns a ``ValidationResult`` for pre-write checks.
    """
    out_df = df
    draft_doc: Optional[OpenDataContractStandard] = None
    if contract:
        ensure_version(contract)
        # validate before write and align schema for write
        result = validate_dataframe(df, contract)
        if not result.ok:
            if draft_on_mismatch:
                ds_id = dataset_id_from_ref(table=table, path=path) if (table or path) else "unknown"
                ds_ver = get_delta_version(df.sparkSession, table=table, path=path) if hasattr(df, 'sparkSession') else None
                draft_doc = _propose_draft_from_dataframe(df, contract, bump=draft_bump, dataset_id=ds_id, dataset_version=ds_ver)
                if draft_store is not None:
                    draft_store.put(draft_doc)
            if enforce:
                raise ValueError(f"Contract validation failed: {result.errors}")
        else:
            out_df = apply_contract(df, contract, auto_cast=auto_cast)
    writer = out_df.write
    if format:
        writer = writer.format(format)
    if options:
        writer = writer.options(**options)
    writer = writer.mode(mode)
    if table:
        writer.saveAsTable(table)
    else:
        if not path:
            raise ValueError("Either table or path must be provided for write")
        writer.save(path)
    vr = ValidationResult(ok=True, errors=[], warnings=[], metrics={})
    return (vr, draft_doc) if return_draft else vr
