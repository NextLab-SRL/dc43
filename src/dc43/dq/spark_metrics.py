from __future__ import annotations

"""Spark-specific metrics and expectation helpers for Data Quality orchestration.

Derives metrics directly from ODCS ``DataQuality`` rules defined on
``SchemaProperty`` and ``SchemaObject`` entries using Spark DataFrames.
Returned metrics use ``violations.*`` keys for rule failures and ``query.*``
for custom SQL checks.
"""

from typing import Any, Dict, List

try:
    from pyspark.sql import DataFrame
except Exception:  # pragma: no cover
    DataFrame = Any  # type: ignore

from open_data_contract_standard.model import OpenDataContractStandard, SchemaProperty  # type: ignore
from .base import DQStatus


def _field_expectations(field: SchemaProperty) -> Dict[str, str]:
    """Return expectation_name -> SQL predicate for a single field."""
    exps: Dict[str, str] = {}
    n = field.name or ""
    if field.required:
        exps[f"not_null_{n}"] = f"{n} IS NOT NULL"
    if field.quality:
        for q in field.quality:
            if q.mustBeGreaterThan is not None:
                exps[f"gt_{n}"] = f"{n} > {q.mustBeGreaterThan}"
            if q.mustBeGreaterOrEqualTo is not None:
                exps[f"ge_{n}"] = f"{n} >= {q.mustBeGreaterOrEqualTo}"
            if q.mustBeLessThan is not None:
                exps[f"lt_{n}"] = f"{n} < {q.mustBeLessThan}"
            if q.mustBeLessOrEqualTo is not None:
                exps[f"le_{n}"] = f"{n} <= {q.mustBeLessOrEqualTo}"
            if q.rule == "enum" and isinstance(q.mustBe, list):
                vals = ", ".join([f"'{v}'" for v in q.mustBe])
                exps[f"enum_{n}"] = f"{n} IN ({vals})"
            if q.rule == "regex" and isinstance(q.mustBe, str):
                exps[f"regex_{n}"] = f"{n} RLIKE '{q.mustBe}'"
    return exps


def expectations_from_contract(contract: OpenDataContractStandard) -> Dict[str, str]:
    """Return expectation_name -> SQL predicate for all fields in a contract."""
    exps: Dict[str, str] = {}
    from ..odcs import list_properties  # local import to avoid cycles

    for f in list_properties(contract):
        exps.update(_field_expectations(f))
    return exps


def compute_metrics(df: DataFrame, contract: OpenDataContractStandard) -> Dict[str, Any]:
    """Compute quality metrics derived from ODCS DataQuality rules."""
    metrics: Dict[str, Any] = {}
    total = df.count()
    metrics["row_count"] = total

    exps = expectations_from_contract(contract)
    for key, expr in exps.items():
        failed = df.filter(f"NOT ({expr})").count()
        metrics[f"violations.{key}"] = failed

    from ..odcs import list_properties  # reuse helper

    for f in list_properties(contract):
        if f.unique or any((q.rule == "unique") for q in (f.quality or [])):
            distinct = df.select(f.name).distinct().count()
            metrics[f"violations.unique_{f.name}"] = total - distinct

    # dataset-level queries in schema quality
    if contract.schema_:
        for obj in contract.schema_:
            if obj.quality:
                for q in obj.quality:
                    if q.query:
                        name = q.name or q.rule or (obj.name or "query")
                        if q.engine and q.engine != "spark_sql":
                            continue
                        try:
                            df.createOrReplaceTempView("_dc43_dq_tmp")
                            row = df.sparkSession.sql(q.query).collect()
                            val = row[0][0] if row else None
                        except Exception:  # pragma: no cover - runtime only
                            val = None
                        metrics[f"query.{name}"] = val

    return metrics


def attach_failed_expectations(
    df: DataFrame,
    contract: OpenDataContractStandard,
    status: DQStatus,
    *,
    collect_examples: bool = False,
    examples_limit: int = 5,
) -> DQStatus:
    """Augment a :class:`~dc43.dq.base.DQStatus` with failed expectations.

    Inspects ``status.details['metrics']`` to identify violated expectations and
    optionally collects example rows for each failure.
    """
    metrics_map = status.details.get("metrics", {}) if status.details else {}
    exps = expectations_from_contract(contract)
    failures: Dict[str, Dict[str, Any]] = {}
    for key, expr in exps.items():
        cnt = metrics_map.get(f"violations.{key}", 0)
        if cnt > 0:
            info: Dict[str, Any] = {"count": cnt, "expression": expr}
            if collect_examples:
                info["examples"] = [
                    r.asDict()
                    for r in df.filter(f"NOT ({expr})").limit(examples_limit).collect()
                ]
            failures[key] = info
    if failures:
        if not status.details:
            status.details = {}
        status.details["failed_expectations"] = failures
    return status
