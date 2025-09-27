"""Runtime adapters that apply contracts inside execution engines."""

from .spark_io import (
    dataset_id_from_ref,
    get_delta_version,
    read_with_contract,
    write_with_contract,
    StaticDatasetLocator,
    ContractFirstDatasetLocator,
    ContractVersionLocator,
)
from .dlt_helpers import expectations_from_contract, apply_dlt_expectations
from .data_quality import (
    SPARK_TYPES,
    attach_failed_expectations,
    build_metrics_payload,
    collect_observations,
    compute_metrics,
    evaluate_observations,
    expectations_from_contract as dq_expectations_from_contract,
    odcs_type_name_from_spark,
    schema_snapshot,
    spark_type_name,
    validate_dataframe,
)
from .validation import apply_contract

__all__ = [
    "dataset_id_from_ref",
    "get_delta_version",
    "read_with_contract",
    "write_with_contract",
    "StaticDatasetLocator",
    "ContractFirstDatasetLocator",
    "ContractVersionLocator",
    "expectations_from_contract",
    "apply_dlt_expectations",
    "SPARK_TYPES",
    "spark_type_name",
    "odcs_type_name_from_spark",
    "schema_snapshot",
    "compute_metrics",
    "collect_observations",
    "evaluate_observations",
    "validate_dataframe",
    "build_metrics_payload",
    "attach_failed_expectations",
    "apply_contract",
    "dq_expectations_from_contract",
]
# `dq_expectations_from_contract` keeps the data-quality expectation helper
# available alongside the DLT-specific ``expectations_from_contract`` export.
