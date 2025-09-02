from __future__ import annotations

"""Validation helpers for Spark DataFrames against ODCS contracts.

This module performs pragmatic checks:
- required columns and optional columns presence
- type compatibility and nullability (best-effort mapping to Spark types)
- common constraints via SQL predicates (enum/regex/min/max)

It can also align a DataFrame to a contract schema (column order and casts).
"""

from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple

try:
    from pyspark.sql import DataFrame
    from pyspark.sql.functions import col
    from pyspark.sql.types import StructType, StructField
except Exception:  # pragma: no cover - allow import without Spark
    DataFrame = Any  # type: ignore
    StructType = Any  # type: ignore
    StructField = Any  # type: ignore
    def col(x):  # type: ignore
        return x
    
from ..odcs import list_properties, field_map
from open_data_contract_standard.model import SchemaProperty, DataQuality  # type: ignore
from open_data_contract_standard.model import OpenDataContractStandard  # type: ignore


@dataclass
class ValidationResult:
    """Result of a validation run.

    - ``ok``: no errors were recorded
    - ``errors``: hard failures (e.g., missing required column)
    - ``warnings``: soft issues (e.g., extra columns when not allowed)
    - ``metrics``: counts of expectation violations and other measures
    """
    ok: bool
    errors: List[str]
    warnings: List[str]
    metrics: Dict[str, Any]


def _schema_from_spark(df: DataFrame) -> Dict[str, Tuple[str, bool]]:
    """Extract a simplified mapping ``name -> (spark_type, nullable)``."""
    schema: Dict[str, Tuple[str, bool]] = {}
    for f in df.schema.fields:  # type: ignore[attr-defined]
        t = str(f.dataType).lower()
        # Normalize a bit: StringType() -> string
        t = (
            t.replace("structfield(", "")
            .replace("stringtype()", "string")
            .replace("longtype()", "bigint")
            .replace("integertype()", "int")
            .replace("booleantype()", "boolean")
            .replace("doubletype()", "double")
            .replace("floattype()", "float")
        )
        schema[f.name] = (t, f.nullable)
    return schema


# Minimal mapping from ODCS primitive type strings to Spark SQL types.
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


def _spark_type(t: str) -> str:
    """Return a Spark SQL type name for a given ODCS primitive type string."""
    return SPARK_TYPES.get(t.lower(), t.lower())


def _build_expectations(field: SchemaProperty) -> Dict[str, str]:
    """Translate a normalized field+constraints to SQL boolean predicates.

    Returned mapping is ``name -> predicate`` suitable for DLT expectations
    and for counting violations (``NOT (predicate)``) in Spark.
    """
    exps: Dict[str, str] = {}
    n = field.name or ""
    # required=True means NOT NULL semantics
    if field.required:
        exps[f"not_null_{n}"] = f"{n} IS NOT NULL"
    # constraints from logicalTypeOptions
    lto = field.logicalTypeOptions or {}
    if isinstance(lto, dict):
        if "enum" in lto and isinstance(lto["enum"], list):
            vals = ", ".join([f"'{v}'" for v in lto["enum"]])
            exps[f"enum_{n}"] = f"{n} IN ({vals})"
        if "pattern" in lto:
            exps[f"regex_{n}"] = f"{n} RLIKE '{lto['pattern']}'"
    # constraints from quality rules
    if field.quality:
        for q in field.quality:
            if q.mustBeGreaterThan is not None:
                exps[f"min_{n}"] = f"{n} > {q.mustBeGreaterThan}"
            if q.mustBeGreaterOrEqualTo is not None:
                exps[f"min_{n}"] = f"{n} >= {q.mustBeGreaterOrEqualTo}"
            if q.mustBeLessThan is not None:
                exps[f"max_{n}"] = f"{n} < {q.mustBeLessThan}"
            if q.mustBeLessOrEqualTo is not None:
                exps[f"max_{n}"] = f"{n} <= {q.mustBeLessOrEqualTo}"
    return exps


def validate_dataframe(
    df: DataFrame,
    contract: OpenDataContractStandard,
    *,
    strict_types: bool = True,
    allow_extra_columns: bool = True,
) -> ValidationResult:
    """Validate a Spark ``DataFrame`` against an ODCS contract (dict/object)."""
    errors: List[str] = []
    warnings: List[str] = []
    metrics: Dict[str, Any] = {}

    # Presence & type checks
    fields = list_properties(contract)
    fmap = {f.name: f for f in fields if f.name}
    spark_schema = _schema_from_spark(df)

    for name, f in fmap.items():
        if name not in spark_schema:
            if f.required:
                errors.append(f"missing required column: {name}")
            else:
                warnings.append(f"missing optional column: {name}")
            continue
        spark_type, nullable = spark_schema[name]
        exp_type = _spark_type((f.physicalType or f.logicalType or "string"))
        if strict_types and exp_type not in spark_type:
            errors.append(f"type mismatch for {name}: expected {exp_type}, got {spark_type}")
        # Spark often marks columns as nullable even when sources have not-null guarantees.
        # Do not treat this as a hard error; rely on not-null expectation checks instead.
        if f.required and nullable:
            warnings.append(f"column {name} marked nullable by Spark but required in contract")

    if not allow_extra_columns:
        extras = [c for c in spark_schema.keys() if c not in fmap]
        if extras:
            warnings.append(f"extra columns present: {extras}")

    # Constraint checks (best effort counts)
    for f in fields:
        exps = _build_expectations(f)
        for key, expr in exps.items():
            try:
                failed = df.filter(f"NOT ({expr})").count()
                metrics[f"violations.{key}"] = failed
                if failed > 0:
                    errors.append(f"expectation failed {key}: {failed} rows")
            except Exception as e:  # pragma: no cover - runtime only
                warnings.append(f"could not evaluate expectation {key}: {e}")

    return ValidationResult(ok=len(errors) == 0, errors=errors, warnings=warnings, metrics=metrics)


def apply_contract(
    df: DataFrame,
    contract: OpenDataContractStandard,
    *,
    auto_cast: bool = True,
    select_only_contract_columns: bool = True,
) -> DataFrame:
    """Return a ``DataFrame`` aligned to the contract schema.

    - Optionally casts types best-effort using Spark SQL ``CAST`` semantics.
    - Selects only columns present in the contract, preserving order.
    """
    cols: List[Any] = []
    for f in list_properties(contract):
        name = f.name
        stype = _spark_type((f.physicalType or f.logicalType or "string"))
        if name in df.columns:
            if auto_cast:
                cols.append(col(name).cast(stype).alias(name))
            else:
                cols.append(col(name))
        else:
            from pyspark.sql.functions import lit

            cols.append(lit(None).cast(stype).alias(name))

    out = df.select(*cols) if select_only_contract_columns else df
    return out
