"""Data-quality engine primitives and Spark-backed helpers."""

from .core import ExpectationSpec, ValidationResult, evaluate_contract, expectation_specs
from .spark import (
    SPARK_TYPES,
    attach_failed_expectations,
    build_metrics_payload,
    collect_observations,
    compute_metrics,
    evaluate_observations,
    expectations_from_contract,
    odcs_type_name_from_spark,
    schema_snapshot,
    spark_type_name,
    validate_dataframe,
)

__all__ = [
    "ExpectationSpec",
    "ValidationResult",
    "evaluate_contract",
    "expectation_specs",
    "SPARK_TYPES",
    "spark_type_name",
    "odcs_type_name_from_spark",
    "schema_snapshot",
    "expectations_from_contract",
    "compute_metrics",
    "collect_observations",
    "evaluate_observations",
    "validate_dataframe",
    "build_metrics_payload",
    "attach_failed_expectations",
]
