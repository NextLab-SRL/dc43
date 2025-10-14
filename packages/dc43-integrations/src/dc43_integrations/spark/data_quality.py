"""Spark-side data-quality integration helpers."""

from __future__ import annotations

from typing import Any, Dict, Iterable, Mapping, Sequence, Tuple

try:  # pragma: no cover - optional dependency
    from pyspark.sql import DataFrame
    from pyspark.sql import functions as F
except Exception:  # pragma: no cover
    DataFrame = Any  # type: ignore
    F = None  # type: ignore

from open_data_contract_standard.model import OpenDataContractStandard  # type: ignore

from dc43_service_clients.data_quality import ValidationResult


# Minimal mapping from ODCS primitive type strings to Spark SQL types.
_CANONICAL_TYPES: Dict[str, str] = {
    "string": "string",
    "bigint": "bigint",
    "int": "int",
    "smallint": "smallint",
    "tinyint": "tinyint",
    "float": "float",
    "double": "double",
    "decimal": "decimal",
    "boolean": "boolean",
    "date": "date",
    "timestamp": "timestamp",
    "binary": "binary",
}

_ALIASED_TYPES: Dict[str, str] = {
    "long": "bigint",
    "integer": "int",
    "short": "smallint",
    "byte": "tinyint",
    "bool": "boolean",
}

SPARK_TYPES: Dict[str, str] = {**_CANONICAL_TYPES, **_ALIASED_TYPES}


def spark_type_name(type_hint: str) -> str:
    """Return a Spark SQL type name for a given ODCS primitive type string."""

    return SPARK_TYPES.get(type_hint.lower(), type_hint.lower())


def _normalize_spark_type(raw: Any) -> str:
    t = str(raw).lower()
    return (
        t.replace("structfield(", "")
        .replace("stringtype()", "string")
        .replace("longtype()", "bigint")
        .replace("integertype()", "int")
        .replace("booleantype()", "boolean")
        .replace("doubletype()", "double")
        .replace("floattype()", "float")
    )


def odcs_type_name_from_spark(raw: Any) -> str:
    """Best-effort mapping from Spark type descriptors to ODCS primitive names."""

    normalized = _normalize_spark_type(raw)
    for odcs_type, spark_name in _CANONICAL_TYPES.items():
        if spark_name in normalized:
            return odcs_type
    for odcs_type, spark_name in _ALIASED_TYPES.items():
        if spark_name in normalized:
            return odcs_type
    return normalized


def schema_snapshot(df: DataFrame) -> Dict[str, Dict[str, Any]]:
    """Return a simplified mapping ``name -> {backend_type, odcs_type, nullable}``."""

    if not hasattr(df, "schema"):
        raise RuntimeError("pyspark is required to inspect DataFrame schema")

    snapshot: Dict[str, Dict[str, Any]] = {}
    for field in df.schema.fields:  # type: ignore[attr-defined]
        snapshot[field.name] = {
            "backend_type": _normalize_spark_type(field.dataType),
            "odcs_type": odcs_type_name_from_spark(field.dataType),
            "nullable": bool(field.nullable),
        }
    return snapshot


ExpectationPlanItem = Mapping[str, Any]
ExpectationPlan = Sequence[ExpectationPlanItem]


def compute_metrics(
    df: DataFrame,
    contract: OpenDataContractStandard,
    *,
    expectations: ExpectationPlan | None = None,
) -> Dict[str, Any]:
    """Compute quality metrics derived from expectation plans."""

    if F is None:  # pragma: no cover - runtime guard
        raise RuntimeError("pyspark is required to compute metrics")

    metrics: Dict[str, Any] = {}
    total = df.count()
    metrics["row_count"] = total

    plan: Iterable[ExpectationPlanItem] = expectations or []
    available_columns = set(df.columns)
    for item in plan:
        if not isinstance(item, Mapping):
            continue
        key = item.get("key")
        rule = str(item.get("rule") or "").lower()
        if not isinstance(key, str) or rule == "query":
            continue
        column = item.get("column")
        predicate = item.get("predicate")
        metric_key = f"violations.{key}"
        if rule == "unique":
            if not isinstance(column, str) or column not in available_columns:
                metrics[metric_key] = total
                continue
            distinct = df.select(column).distinct().count()
            metrics[metric_key] = total - distinct
            continue
        if not isinstance(predicate, str):
            continue
        if isinstance(column, str) and column not in available_columns:
            metrics[metric_key] = total
            continue
        failed = df.filter(f"NOT ({predicate})").count()
        metrics[metric_key] = failed

    for item in plan:
        if not isinstance(item, Mapping):
            continue
        rule = str(item.get("rule") or "").lower()
        if rule != "query":
            continue
        key = item.get("key")
        if not isinstance(key, str):
            continue
        params = item.get("params")
        params_map: Mapping[str, Any] = params if isinstance(params, Mapping) else {}
        query = params_map.get("query")
        if not query:
            continue
        engine = str(params_map.get("engine") or "spark_sql").lower()
        if engine and engine not in {"spark", "spark_sql"}:
            continue
        try:
            df.createOrReplaceTempView("_dc43_dq_tmp")
            row = df.sparkSession.sql(str(query)).collect()
            val = row[0][0] if row else None
        except Exception:  # pragma: no cover - runtime only
            val = None
        metrics[f"query.{key}"] = val

    return metrics


def collect_observations(
    df: DataFrame,
    contract: OpenDataContractStandard,
    *,
    collect_metrics: bool = True,
    expectations: ExpectationPlan | None = None,
) -> Tuple[Dict[str, Dict[str, Any]], Dict[str, Any]]:
    """Return (schema, metrics) tuples gathered from a Spark DataFrame."""

    schema = schema_snapshot(df)
    metrics: Dict[str, Any] = {}
    if collect_metrics:
        metrics = compute_metrics(df, contract, expectations=expectations)
    return schema, metrics


def build_metrics_payload(
    df: DataFrame,
    contract: OpenDataContractStandard,
    *,
    validation: ValidationResult | None = None,
    include_schema: bool = True,
    expectations: ExpectationPlan | None = None,
    collect_metrics: bool = True,
) -> Tuple[Dict[str, Any], Dict[str, Dict[str, Any]], bool]:
    """Return ``(metrics, schema, reused)`` suitable for governance submission."""

    metrics = dict(validation.metrics) if validation and validation.metrics else {}
    schema = dict(validation.schema) if validation and validation.schema else {}
    reused = bool(metrics)

    plan: ExpectationPlan | None = expectations
    if plan is None and validation is not None:
        details_plan = validation.details.get("expectation_plan")
        if isinstance(details_plan, Iterable):
            plan = [item for item in details_plan if isinstance(item, Mapping)]

    if collect_metrics and not metrics:
        metrics = compute_metrics(df, contract, expectations=plan)
    if include_schema and not schema:
        schema = schema_snapshot(df)
    if include_schema and schema and "schema" not in metrics:
        metrics = dict(metrics)
        metrics["schema"] = schema

    return metrics, schema, reused


def attach_failed_expectations(
    contract: OpenDataContractStandard,
    status: ValidationResult,
    *,
    metrics: Mapping[str, Any] | None = None,
    expectations: ExpectationPlan | None = None,
) -> ValidationResult:
    """Augment ``status`` with failed expectations derived from engine metrics."""

    metrics_map: Dict[str, Any] = {}
    if metrics:
        metrics_map.update(dict(metrics))
    if status.metrics:
        metrics_map.update(status.metrics)
    if status.details:
        metrics_map.update(status.details.get("metrics", {}))
    plan: list[ExpectationPlanItem] = []
    if expectations:
        plan.extend(expectations)
    else:
        raw_plan = status.details.get("expectation_plan")
        if isinstance(raw_plan, Iterable):
            plan.extend(item for item in raw_plan if isinstance(item, Mapping))
    failures: Dict[str, Dict[str, Any]] = {}
    for item in plan:
        if not isinstance(item, Mapping):
            continue
        key = item.get("key")
        if not isinstance(key, str):
            continue
        rule = str(item.get("rule") or "").lower()
        if rule == "query":
            continue
        metric_key = f"violations.{key}"
        cnt = metrics_map.get(metric_key, 0)
        if not isinstance(cnt, (int, float)) or cnt <= 0:
            continue
        info: Dict[str, Any] = {"count": int(cnt)}
        predicate = item.get("predicate")
        column = item.get("column")
        if isinstance(predicate, str):
            info["expression"] = predicate
        if isinstance(column, str) and column:
            info["column"] = column
        failures[key] = info
    if failures:
        status.merge_details({"failed_expectations": failures})
    return status


__all__ = [
    "SPARK_TYPES",
    "spark_type_name",
    "odcs_type_name_from_spark",
    "schema_snapshot",
    "compute_metrics",
    "collect_observations",
    "build_metrics_payload",
    "attach_failed_expectations",
]
