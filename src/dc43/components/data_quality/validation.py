from __future__ import annotations

"""Validation helpers for Spark DataFrames against ODCS contracts.

This module performs pragmatic checks:
- required columns and optional columns presence
- type compatibility and nullability (best-effort mapping to Spark types)

It can also align a DataFrame to a contract schema (column order and casts).
"""

from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.functions import col

from dc43.odcs import list_properties
from open_data_contract_standard.model import SchemaProperty  # type: ignore
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

    @property
    def details(self) -> Dict[str, Any]:
        """Structured representation combining errors, warnings and metrics."""

        return {"errors": self.errors, "warnings": self.warnings, "metrics": self.metrics}


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

    # For required columns Spark often reports ``nullable=True`` even when the
    # underlying source never produces null values (e.g. JSON inference).  When
    # this happens we inspect the actual data once so we can distinguish genuine
    # violations from metadata noise.
    nullable_required: List[str] = []
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
        if f.required and nullable:
            nullable_required.append(name)

    null_counts: Dict[str, int] = {}
    if nullable_required:
        try:
            aggregations = [
                F.sum(F.when(col(name).isNull(), 1).otherwise(0)).alias(name)
                for name in nullable_required
            ]
            # ``collect`` is safe here because the result is a single row with
            # aggregated counts, keeping the validation overhead minimal even
            # for large datasets.
            counts = df.select(*aggregations).collect()[0].asDict()
            null_counts = {name: int(counts.get(name) or 0) for name in nullable_required}
        except Exception:  # pragma: no cover - defensively handle Spark errors
            # Fallback to flagging a warning when counts cannot be computed.
            null_counts = {name: -1 for name in nullable_required}

    for name, f in fmap.items():
        if name not in spark_schema:
            continue
        _, nullable = spark_schema[name]
        if f.required and nullable:
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
        extras = [c for c in spark_schema.keys() if c not in fmap]
        if extras:
            warnings.append(f"extra columns present: {extras}")

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
        if not name:
            continue
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


__all__ = [
    "ValidationResult",
    "validate_dataframe",
    "apply_contract",
    "SPARK_TYPES",
]
