from __future__ import annotations

"""Metrics computation for Data Quality orchestration.

Given a metric specification provided by a DQ client (per field list of
metrics to compute), produce aggregated counts and values on a Spark
``DataFrame``. Also emits compatibility keys like ``violations.*`` that
the DQ stub uses to decide statuses.
"""

from typing import Any, Dict, List

try:
    from pyspark.sql import DataFrame
    from pyspark.sql import functions as F
except Exception:  # pragma: no cover
    DataFrame = Any  # type: ignore
    F = None  # type: ignore

from ..odcs import list_properties
from open_data_contract_standard.model import OpenDataContractStandard  # type: ignore


def compute_metrics(df: DataFrame, contract: OpenDataContractStandard, metric_spec: Dict[str, Any]) -> Dict[str, Any]:
    """Compute metrics according to a spec for fields in the contract.

    Supported metrics: ``null_count``, ``min``, ``max``, ``distinct_count``,
    ``regex_violations`` (pattern), ``enum_violations`` (allowed set).
    """
    metrics: Dict[str, Any] = {}

    if metric_spec.get("row_count"):
        metrics["row_count"] = df.count()

    fields_spec = metric_spec.get("fields", {}) or {}
    # Build aggregations in one pass where possible
    aggs = []
    agg_names: List[str] = []

    c_fields = {f.name for f in list_properties(contract)}
    for name in c_fields:
        if name not in fields_spec:
            continue
        specs = fields_spec[name]
        for spec in specs:
            if spec == "null_count":
                aggs.append(F.sum(F.when(F.col(name).isNull(), 1).otherwise(0)).alias(f"null_count__{name}"))
                agg_names.append(f"null_count__{name}")
            elif spec == "min":
                aggs.append(F.min(F.col(name)).alias(f"min__{name}"))
                agg_names.append(f"min__{name}")
            elif spec == "max":
                aggs.append(F.max(F.col(name)).alias(f"max__{name}"))
                agg_names.append(f"max__{name}")
            elif spec == "distinct_count":
                aggs.append(F.countDistinct(F.col(name)).alias(f"distinct_count__{name}"))
                agg_names.append(f"distinct_count__{name}")
            elif isinstance(spec, dict) and "regex_violations" in spec:
                regex = spec["regex_violations"]
                aggs.append(F.sum(F.when(~F.col(name).rlike(regex), 1).otherwise(0)).alias(f"regex_violations__{name}"))
                agg_names.append(f"regex_violations__{name}")
            elif isinstance(spec, dict) and "enum_violations" in spec:
                allowed = spec["enum_violations"]
                aggs.append(F.sum(F.when(~F.col(name).isin(allowed), 1).otherwise(0)).alias(f"enum_violations__{name}"))
                agg_names.append(f"enum_violations__{name}")

    if aggs:
        row = df.agg(*aggs).collect()[0]
        for name in agg_names:
            metrics[name] = row[name]

    # For compatibility with validation expectations
    for k, v in list(metrics.items()):
        if k.startswith("regex_violations__"):
            metrics[f"violations.regex_{k.split('__',1)[1]}"] = v
        if k.startswith("enum_violations__"):
            metrics[f"violations.enum_{k.split('__',1)[1]}"] = v

    return metrics
