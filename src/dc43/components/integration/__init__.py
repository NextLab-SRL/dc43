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
]
