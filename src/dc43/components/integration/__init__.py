"""Runtime adapters that apply contracts inside execution engines."""

from .spark_io import read_with_contract, write_with_contract
from .validation import ValidationResult, validate_dataframe, apply_contract
from .dataset import get_delta_version, dataset_id_from_ref
from .dlt_helpers import expectations_from_contract, apply_dlt_expectations

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
