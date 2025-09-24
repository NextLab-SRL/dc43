"""Spark-specific helpers that collect observations for the DQ engine."""

from __future__ import annotations

from typing import Any, Dict, List, Mapping, Tuple

try:  # pragma: no cover - optional dependency
    from pyspark.sql import DataFrame
    from pyspark.sql import functions as F
    from pyspark.sql.functions import col
except Exception:  # pragma: no cover
    DataFrame = Any  # type: ignore
    F = None  # type: ignore
    col = None  # type: ignore

from open_data_contract_standard.model import (  # type: ignore
    OpenDataContractStandard,
    SchemaProperty,
)

from dc43.components.data_quality.engine import ValidationResult, evaluate_contract
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


def _field_expectations(field: SchemaProperty) -> Dict[str, Tuple[str, str]]:
    """Return expectation_name -> (SQL predicate, column name) for a single field."""

    expectations: Dict[str, Tuple[str, str]] = {}
    name = field.name or ""
    if not name:
        return expectations
    if field.required:
        expectations[f"not_null_{name}"] = (f"{name} IS NOT NULL", name)
    if field.quality:
        for q in field.quality:
            if q.mustBeGreaterThan is not None:
                expectations[f"gt_{name}"] = (f"{name} > {q.mustBeGreaterThan}", name)
            if q.mustBeGreaterOrEqualTo is not None:
                expectations[f"ge_{name}"] = (f"{name} >= {q.mustBeGreaterOrEqualTo}", name)
            if q.mustBeLessThan is not None:
                expectations[f"lt_{name}"] = (f"{name} < {q.mustBeLessThan}", name)
            if q.mustBeLessOrEqualTo is not None:
                expectations[f"le_{name}"] = (f"{name} <= {q.mustBeLessOrEqualTo}", name)
            if q.rule == "enum" and isinstance(q.mustBe, list):
                vals = ", ".join([f"'{v}'" for v in q.mustBe])
                expectations[f"enum_{name}"] = (f"{name} IN ({vals})", name)
            if q.rule == "regex" and isinstance(q.mustBe, str):
                expectations[f"regex_{name}"] = (f"{name} RLIKE '{q.mustBe}'", name)
    return expectations


def field_expectations(contract: OpenDataContractStandard) -> Dict[str, Tuple[str, str]]:
    """Return expectation_name -> (SQL predicate, column name) for the contract."""

    expectations: Dict[str, Tuple[str, str]] = {}
    for prop in list_properties(contract):
        expectations.update(_field_expectations(prop))
    return expectations


def expectations_from_contract(contract: OpenDataContractStandard) -> Dict[str, str]:
    """Return expectation_name -> SQL predicate for all fields in a contract."""

    return {key: expr for key, (expr, _) in field_expectations(contract).items()}


def compute_metrics(df: DataFrame, contract: OpenDataContractStandard) -> Dict[str, Any]:
    """Compute quality metrics derived from ODCS DataQuality rules."""

    if F is None:  # pragma: no cover - runtime guard
        raise RuntimeError("pyspark is required to compute metrics")

    metrics: Dict[str, Any] = {}
    total = df.count()
    metrics["row_count"] = total

    expectations = field_expectations(contract)
    available_columns = set(df.columns)
    for key, (expr, column) in expectations.items():
        if column and column not in available_columns:
            metrics[f"violations.{key}"] = total
            continue
        failed = df.filter(f"NOT ({expr})").count()
        metrics[f"violations.{key}"] = failed

    for field in list_properties(contract):
        if field.unique or any((q.rule == "unique") for q in (field.quality or [])):
            if not field.name:
                continue
            if field.name not in available_columns:
                metrics[f"violations.unique_{field.name}"] = total
                continue
            distinct = df.select(field.name).distinct().count()
            metrics[f"violations.unique_{field.name}"] = total - distinct

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
) -> ValidationResult:
    """Validate ``df`` against ``contract`` using Spark-collected observations."""

    schema, metrics = collect_observations(df, contract, collect_metrics=collect_metrics)
    return evaluate_contract(
        contract,
        schema=schema,
        metrics=metrics,
        strict_types=strict_types,
        allow_extra_columns=allow_extra_columns,
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
    *,
    collect_examples: bool = False,
    examples_limit: int = 5,
) -> DQStatus:
    """Augment ``status`` with failed expectations derived from Spark metrics."""

    metrics_map = status.details.get("metrics", {}) if status.details else {}
    expectations = field_expectations(contract)
    failures: Dict[str, Dict[str, Any]] = {}
    for key, (expr_text, column) in expectations.items():
        cnt = metrics_map.get(f"violations.{key}", 0)
        if cnt and cnt > 0:
            info: Dict[str, Any] = {"count": cnt, "expression": expr_text}
            if collect_examples and column in df.columns:
                info["examples"] = [
                    row.asDict()
                    for row in df.filter(f"NOT ({expr_text})").limit(examples_limit).collect()
                ]
            failures[key] = info
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


def draft_from_dataframe(
    df: DataFrame,
    base_contract: OpenDataContractStandard,
    *,
    metrics: Mapping[str, Any] | None = None,
    collect_metrics: bool = False,
    bump: str = "minor",
    dataset_id: str | None = None,
    dataset_version: str | None = None,
    data_format: str | None = None,
    dq_feedback: Dict[str, Any] | None = None,
) -> OpenDataContractStandard:
    """Convenience wrapper that drafts a contract from a Spark DataFrame."""

    schema = schema_snapshot(df)
    metrics_payload = dict(metrics) if metrics else {}
    if collect_metrics and not metrics_payload:
        metrics_payload = compute_metrics(df, base_contract)

    from dc43.components.contract_drafter.observations import draft_from_observations

    return draft_from_observations(
        schema=schema,
        metrics=metrics_payload or None,
        base_contract=base_contract,
        bump=bump,
        dataset_id=dataset_id,
        dataset_version=dataset_version,
        data_format=data_format,
        dq_feedback=dq_feedback,
    )


__all__ = [
    "SPARK_TYPES",
    "spark_type_name",
    "odcs_type_name_from_spark",
    "schema_snapshot",
    "field_expectations",
    "expectations_from_contract",
    "compute_metrics",
    "collect_observations",
    "validate_dataframe",
    "build_metrics_payload",
    "attach_failed_expectations",
    "apply_contract",
    "draft_from_dataframe",
]
