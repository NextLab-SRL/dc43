"""Compatibility exports for runtime integration helpers."""

from dc43.components.integration import (
    read_with_contract,
    write_with_contract,
    ValidationResult,
    validate_dataframe,
    apply_contract,
    get_delta_version,
    dataset_id_from_ref,
    expectations_from_contract,
    apply_dlt_expectations,
)

__all__ = [
    "read_with_contract",
    "write_with_contract",
    "ValidationResult",
    "validate_dataframe",
    "apply_contract",
    "get_delta_version",
    "dataset_id_from_ref",
    "expectations_from_contract",
    "apply_dlt_expectations",
]
