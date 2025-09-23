"""Spark alignment helpers used by the integration layer.

Schema validation lives in :mod:`dc43.components.data_quality.engine.spark` so the
execution engine can emit metrics and schema snapshots for governance.  This module
keeps the convenience helper that reshapes a Spark ``DataFrame`` to match a contract
schema before reads or writes.
"""

from __future__ import annotations

from typing import Any, List

try:  # pragma: no cover - optional dependency
    from pyspark.sql import DataFrame
    from pyspark.sql.functions import col
except Exception:  # pragma: no cover
    DataFrame = Any  # type: ignore
    col = None  # type: ignore

from open_data_contract_standard.model import OpenDataContractStandard  # type: ignore

from dc43.odcs import list_properties

from .engine import (
    ValidationResult,
    schema_snapshot,
    spark_type_name,
    validate_dataframe,
)


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

    if col is None:  # pragma: no cover - runtime guard
        raise RuntimeError("pyspark is required to apply a contract to a DataFrame")

    contract_column_names: List[str] = []
    contract_exprs: List[Any] = []
    for field in list_properties(contract):
        name = field.name
        if not name:
            continue
        contract_column_names.append(name)
        stype = spark_type_name(field.physicalType or field.logicalType or "string")
        if name in df.columns:
            if auto_cast:
                contract_exprs.append(col(name).cast(stype).alias(name))
            else:
                contract_exprs.append(col(name))
        else:
            from pyspark.sql.functions import lit

            contract_exprs.append(lit(None).cast(stype).alias(name))

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
    "ValidationResult",
    "validate_dataframe",
    "schema_snapshot",
    "spark_type_name",
    "apply_contract",
]
