"""Spark-specific helpers that collect observations for the DQ engine."""

from __future__ import annotations

from typing import Any, Dict, List, Literal, Mapping, Tuple

try:  # pragma: no cover - optional dependency
    from pyspark.sql import DataFrame
    from pyspark.sql import functions as F
    from pyspark.sql.functions import col
except Exception:  # pragma: no cover
    DataFrame = Any  # type: ignore
    F = None  # type: ignore
    col = None  # type: ignore

from open_data_contract_standard.model import OpenDataContractStandard  # type: ignore

from dc43.components.data_quality.engine import (
    ExpectationSpec,
    ValidationResult,
    evaluate_contract,
    expectation_specs,
)
from dc43.components.data_quality.governance import DQStatus
from dc43.odcs import list_properties


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


def spark_type_name(type_hint: str) -> str:
    """Return a Spark SQL type name for a given ODCS primitive type string."""

    return SPARK_TYPES.get(type_hint.lower(), type_hint.lower())


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


def _sql_literal(value: Any) -> str:
    if isinstance(value, str):
        escaped = value.replace("'", "\\'")
        return f"'{escaped}'"
    if value is None:
        return "NULL"
    return str(value)


def _sql_predicate(spec: ExpectationSpec) -> str | None:
    column = spec.column
    if not column:
        return None
    if spec.rule in {"not_null", "required"}:
        return f"{column} IS NOT NULL"
    if spec.rule == "gt":
        return f"{column} > {_sql_literal(spec.params.get('threshold'))}"
    if spec.rule == "ge":
        return f"{column} >= {_sql_literal(spec.params.get('threshold'))}"
    if spec.rule == "lt":
        return f"{column} < {_sql_literal(spec.params.get('threshold'))}"
    if spec.rule == "le":
        return f"{column} <= {_sql_literal(spec.params.get('threshold'))}"
    if spec.rule == "enum":
        values = spec.params.get("values") or []
        if not isinstance(values, (list, tuple, set)):
            return None
        literals = ", ".join(_sql_literal(v) for v in values)
        return f"{column} IN ({literals})" if literals else None
    if spec.rule == "regex":
        pattern = spec.params.get("pattern")
        if pattern is None:
            return None
        pattern_str = str(pattern).replace("'", "\\'")
        return f"{column} RLIKE '{pattern_str}'"
    return None


def expectations_from_contract(contract: OpenDataContractStandard) -> Dict[str, str]:
    """Return expectation_name -> SQL predicate for all evaluable rules."""

    mapping: Dict[str, str] = {}
    for spec in expectation_specs(contract):
        predicate = _sql_predicate(spec)
        if predicate:
            mapping[spec.key] = predicate
    return mapping


def compute_metrics(df: DataFrame, contract: OpenDataContractStandard) -> Dict[str, Any]:
    """Compute quality metrics derived from ODCS DataQuality rules."""

    if F is None:  # pragma: no cover - runtime guard
        raise RuntimeError("pyspark is required to compute metrics")

    metrics: Dict[str, Any] = {}
    total = df.count()
    metrics["row_count"] = total

    available_columns = set(df.columns)
    specs = expectation_specs(contract)
    for spec in specs:
        if spec.rule == "query":
            continue
        if spec.rule == "unique":
            column = spec.column
            if not column:
                continue
            if column not in available_columns:
                metrics[f"violations.{spec.key}"] = total
                continue
            distinct = df.select(column).distinct().count()
            metrics[f"violations.{spec.key}"] = total - distinct
            continue
        predicate = _sql_predicate(spec)
        if not predicate:
            continue
        column = spec.column
        if column and column not in available_columns:
            metrics[f"violations.{spec.key}"] = total
            continue
        failed = df.filter(f"NOT ({predicate})").count()
        metrics[f"violations.{spec.key}"] = failed

    for spec in specs:
        if spec.rule != "query":
            continue
        query = spec.params.get("query")
        if not query:
            continue
        engine = (spec.params.get("engine") or "spark_sql").lower()
        if engine and engine not in {"spark", "spark_sql"}:
            continue
        try:
            df.createOrReplaceTempView("_dc43_dq_tmp")
            row = df.sparkSession.sql(query).collect()
            val = row[0][0] if row else None
        except Exception:  # pragma: no cover - runtime only
            val = None
        metrics[f"query.{spec.key}"] = val

    return metrics


def collect_observations(
    df: DataFrame,
    contract: OpenDataContractStandard,
    *,
    collect_metrics: bool = True,
) -> Tuple[Dict[str, Dict[str, Any]], Dict[str, Any]]:
    """Return (schema, metrics) tuples gathered from a Spark DataFrame."""

    schema = schema_snapshot(df)
    metrics: Dict[str, Any] = {}
    if collect_metrics:
        metrics = compute_metrics(df, contract)
    return schema, metrics


def validate_dataframe(
    df: DataFrame,
    contract: OpenDataContractStandard,
    *,
    strict_types: bool = True,
    allow_extra_columns: bool = True,
    collect_metrics: bool = True,
    expectation_severity: Literal["error", "warning", "ignore"] = "warning",
) -> ValidationResult:
    """Validate ``df`` against ``contract`` using Spark-collected observations."""

    schema, metrics = collect_observations(df, contract, collect_metrics=collect_metrics)
    return evaluate_contract(
        contract,
        schema=schema,
        metrics=metrics,
        strict_types=strict_types,
        allow_extra_columns=allow_extra_columns,
        expectation_severity=expectation_severity,
    )


def build_metrics_payload(
    df: DataFrame,
    contract: OpenDataContractStandard,
    *,
    validation: ValidationResult | None = None,
    include_schema: bool = True,
) -> Tuple[Dict[str, Any], Dict[str, Dict[str, Any]], bool]:
    """Return ``(metrics, schema, reused)`` suitable for governance submission."""

    metrics = dict(validation.metrics) if validation and validation.metrics else {}
    schema = dict(validation.schema) if validation and validation.schema else {}
    reused = bool(metrics)

    if not metrics:
        metrics = compute_metrics(df, contract)
    if include_schema and not schema:
        schema = schema_snapshot(df)
    if include_schema and schema and "schema" not in metrics:
        metrics = dict(metrics)
        metrics["schema"] = schema

    return metrics, schema, reused


def attach_failed_expectations(
    df: DataFrame,
    contract: OpenDataContractStandard,
    status: DQStatus,
) -> DQStatus:
    """Augment ``status`` with failed expectations derived from Spark metrics."""

    metrics_map = status.details.get("metrics", {}) if status.details else {}
    specs = expectation_specs(contract)
    failures: Dict[str, Dict[str, Any]] = {}
    for spec in specs:
        if spec.rule == "query":
            continue
        metric_key = f"violations.{spec.key}"
        cnt = metrics_map.get(metric_key, 0)
        if not isinstance(cnt, (int, float)) or cnt <= 0:
            continue
        expr = _sql_predicate(spec)
        info: Dict[str, Any] = {"count": int(cnt)}
        if expr:
            info["expression"] = expr
        if spec.column:
            info["column"] = spec.column
        failures[spec.key] = info
    if failures:
        if not status.details:
            status.details = {}
        status.details["failed_expectations"] = failures
    return status


def apply_contract(
    df: DataFrame,
    contract: OpenDataContractStandard,
    *,
    auto_cast: bool = True,
    select_only_contract_columns: bool = True,
) -> DataFrame:
    """Return a ``DataFrame`` aligned to the contract schema."""

    if col is None:  # pragma: no cover - runtime guard
        raise RuntimeError("pyspark is required to apply a contract to a DataFrame")

    contract_column_names: List[str] = []
    contract_exprs: List[Any] = []
    for field in list_properties(contract):
        name = field.name
        if not name:
            continue
        contract_column_names.append(name)
        target_type = spark_type_name(field.physicalType or field.logicalType or "string")
        if name in df.columns:
            if auto_cast:
                contract_exprs.append(col(name).cast(target_type).alias(name))
            else:
                contract_exprs.append(col(name))
        else:
            from pyspark.sql.functions import lit

            contract_exprs.append(lit(None).cast(target_type).alias(name))

    if not contract_exprs:
        return df

    contract_df = df.select(*contract_exprs)
    if select_only_contract_columns:
        return contract_df

    remaining = [col(c) for c in df.columns if c not in contract_column_names]
    if not remaining:
        return contract_df
    return df.select(*contract_exprs, *remaining)


__all__ = [
    "SPARK_TYPES",
    "spark_type_name",
    "odcs_type_name_from_spark",
    "schema_snapshot",
    "expectations_from_contract",
    "compute_metrics",
    "collect_observations",
    "validate_dataframe",
    "build_metrics_payload",
    "attach_failed_expectations",
    "apply_contract",
]
