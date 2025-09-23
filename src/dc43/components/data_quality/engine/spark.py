"""Spark-specific metrics, schema introspection, and helpers for DQ orchestration."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, List, Tuple

try:  # pragma: no cover - optional dependency
    from pyspark.sql import DataFrame
    from pyspark.sql import functions as F
    from pyspark.sql.functions import col
except Exception:  # pragma: no cover
    DataFrame = Any  # type: ignore
    F = None  # type: ignore
    col = None  # type: ignore

from open_data_contract_standard.model import OpenDataContractStandard, SchemaProperty  # type: ignore

from dc43.components.data_quality.governance import DQStatus
from dc43.odcs import list_properties


SPARK_TYPES = {
    "string": "string",
    "int": "int",
    "integer": "int",
    "long": "bigint",
    "bigint": "bigint",
    "short": "smallint",
    "byte": "tinyint",
    "float": "float",
    "double": "double",
    "decimal": "decimal",
    "boolean": "boolean",
    "bool": "boolean",
    "date": "date",
    "timestamp": "timestamp",
    "binary": "binary",
}


def spark_type_name(type_hint: str) -> str:
    """Return a Spark SQL type name for a given ODCS primitive type string."""

    return SPARK_TYPES.get(type_hint.lower(), type_hint.lower())


def odcs_type_name_from_spark(raw: Any) -> str:
    """Best-effort mapping from Spark type descriptors to ODCS primitive names."""

    normalized = _normalize_spark_type(raw)
    for odcs_type, spark_name in SPARK_TYPES.items():
        if spark_name in normalized:
            return odcs_type
    return normalized


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


def schema_snapshot(df: DataFrame) -> Dict[str, Dict[str, Any]]:
    """Return a simplified mapping ``name -> {spark_type, nullable}``."""

    if not hasattr(df, "schema"):
        raise RuntimeError("pyspark is required to inspect DataFrame schema")

    snapshot: Dict[str, Dict[str, Any]] = {}
    for field in df.schema.fields:  # type: ignore[attr-defined]
        snapshot[field.name] = {
            "spark_type": _normalize_spark_type(field.dataType),
            "nullable": bool(field.nullable),
        }
    return snapshot


@dataclass
class ValidationResult:
    """Result of a validation run executed by the Spark DQ engine.

    ``metrics`` contains expectation counters and any dataset-level
    observations, while ``schema`` captures the Spark-inferred schema used
    during validation so governance adapters can store or replay verdicts.
    """

    ok: bool
    errors: List[str]
    warnings: List[str]
    metrics: Dict[str, Any] = field(default_factory=dict)
    schema: Dict[str, Dict[str, Any]] = field(default_factory=dict)

    @property
    def details(self) -> Dict[str, Any]:
        """Structured representation combining validation observations."""

        return {
            "errors": self.errors,
            "warnings": self.warnings,
            "metrics": self.metrics,
            "schema": self.schema,
        }


def _field_expectations(field: SchemaProperty) -> Dict[str, Tuple[str, str]]:
    """Return expectation_name -> (SQL predicate, column name) for a single field."""

    exps: Dict[str, Tuple[str, str]] = {}
    name = field.name or ""
    if not name:
        return exps
    if field.required:
        exps[f"not_null_{name}"] = (f"{name} IS NOT NULL", name)
    if field.quality:
        for q in field.quality:
            if q.mustBeGreaterThan is not None:
                exps[f"gt_{name}"] = (f"{name} > {q.mustBeGreaterThan}", name)
            if q.mustBeGreaterOrEqualTo is not None:
                exps[f"ge_{name}"] = (f"{name} >= {q.mustBeGreaterOrEqualTo}", name)
            if q.mustBeLessThan is not None:
                exps[f"lt_{name}"] = (f"{name} < {q.mustBeLessThan}", name)
            if q.mustBeLessOrEqualTo is not None:
                exps[f"le_{name}"] = (f"{name} <= {q.mustBeLessOrEqualTo}", name)
            if q.rule == "enum" and isinstance(q.mustBe, list):
                vals = ", ".join([f"'{v}'" for v in q.mustBe])
                exps[f"enum_{name}"] = (f"{name} IN ({vals})", name)
            if q.rule == "regex" and isinstance(q.mustBe, str):
                exps[f"regex_{name}"] = (f"{name} RLIKE '{q.mustBe}'", name)
    return exps


def field_expectations(contract: OpenDataContractStandard) -> Dict[str, Tuple[str, str]]:
    """Return expectation_name -> (SQL predicate, column name) for the contract."""

    exps: Dict[str, Tuple[str, str]] = {}
    for f in list_properties(contract):
        exps.update(_field_expectations(f))
    return exps


def expectations_from_contract(contract: OpenDataContractStandard) -> Dict[str, str]:
    """Return expectation_name -> SQL predicate for all fields in a contract."""

    return {key: expr for key, (expr, _) in field_expectations(contract).items()}


def compute_metrics(df: DataFrame, contract: OpenDataContractStandard) -> Dict[str, Any]:
    """Compute quality metrics derived from ODCS DataQuality rules."""

    metrics: Dict[str, Any] = {}
    total = df.count()
    metrics["row_count"] = total

    exps = field_expectations(contract)
    available_columns = set(df.columns)
    for key, (expr, column) in exps.items():
        if column and column not in available_columns:
            metrics[f"violations.{key}"] = total
            continue
        failed = df.filter(f"NOT ({expr})").count()
        metrics[f"violations.{key}"] = failed

    for f in list_properties(contract):
        if f.unique or any((q.rule == "unique") for q in (f.quality or [])):
            if not f.name:
                continue
            if f.name not in available_columns:
                metrics[f"violations.unique_{f.name}"] = total
                continue
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


def validate_dataframe(
    df: DataFrame,
    contract: OpenDataContractStandard,
    *,
    strict_types: bool = True,
    allow_extra_columns: bool = True,
    collect_metrics: bool = True,
) -> ValidationResult:
    """Validate a Spark ``DataFrame`` against an ODCS contract using the DQ engine."""

    if F is None or col is None:  # pragma: no cover - runtime guard
        raise RuntimeError("pyspark is required to validate Spark DataFrames")

    errors: List[str] = []
    warnings: List[str] = []

    schema_info = schema_snapshot(df)

    fields = list_properties(contract)
    fmap = {f.name: f for f in fields if f.name}

    nullable_required: List[str] = []
    for name, field in fmap.items():
        if name not in schema_info:
            if field.required:
                errors.append(f"missing required column: {name}")
            else:
                warnings.append(f"missing optional column: {name}")
            continue
        spark_type = schema_info[name]["spark_type"]
        expected = spark_type_name(field.physicalType or field.logicalType or "string")
        if strict_types and expected not in spark_type:
            errors.append(f"type mismatch for {name}: expected {expected}, got {spark_type}")
        if field.required and schema_info[name]["nullable"]:
            nullable_required.append(name)

    null_counts: Dict[str, int] = {}
    if nullable_required:
        try:
            aggregations = [
                F.sum(F.when(col(name).isNull(), 1).otherwise(0)).alias(name)  # type: ignore[attr-defined]
                for name in nullable_required
            ]
            counts = df.select(*aggregations).collect()[0].asDict()
            null_counts = {name: int(counts.get(name) or 0) for name in nullable_required}
        except Exception:  # pragma: no cover - defensively handle Spark errors
            null_counts = {name: -1 for name in nullable_required}

    for name, field in fmap.items():
        if name not in schema_info:
            continue
        if field.required and schema_info[name]["nullable"]:
            nulls = null_counts.get(name)
            if nulls is None:
                warnings.append(
                    f"column {name} marked nullable by Spark but required in contract"
                )
            elif nulls < 0:
                warnings.append(
                    f"column {name} required by contract but nullability could not be verified"
                )
            elif nulls > 0:
                errors.append(
                    f"column {name} contains {nulls} null value(s) but is required in the contract"
                )

    if not allow_extra_columns:
        extras = [name for name in schema_info.keys() if name not in fmap]
        if extras:
            warnings.append(f"extra columns present: {extras}")

    metrics: Dict[str, Any] = {}
    if collect_metrics:
        metrics = compute_metrics(df, contract)

    return ValidationResult(
        ok=len(errors) == 0,
        errors=errors,
        warnings=warnings,
        metrics=metrics,
        schema=schema_info,
    )


def attach_failed_expectations(
    df: DataFrame,
    contract: OpenDataContractStandard,
    status: DQStatus,
    *,
    collect_examples: bool = False,
    examples_limit: int = 5,
) -> DQStatus:
    """Augment a :class:`~dc43.components.data_quality.governance.interface.DQStatus` with failed expectations."""

    metrics_map = status.details.get("metrics", {}) if status.details else {}
    exps = field_expectations(contract)
    failures: Dict[str, Dict[str, Any]] = {}
    for key, expr in exps.items():
        expr_text, column = expr
        cnt = metrics_map.get(f"violations.{key}", 0)
        if cnt > 0:
            info: Dict[str, Any] = {"count": cnt, "expression": expr_text}
            if collect_examples and column in df.columns:
                info["examples"] = [
                    r.asDict()
                    for r in df.filter(f"NOT ({expr_text})").limit(examples_limit).collect()
                ]
            failures[key] = info
    if failures:
        if not status.details:
            status.details = {}
        status.details["failed_expectations"] = failures
    return status


__all__ = [
    "ValidationResult",
    "schema_snapshot",
    "spark_type_name",
    "odcs_type_name_from_spark",
    "expectations_from_contract",
    "compute_metrics",
    "attach_failed_expectations",
    "validate_dataframe",
]
