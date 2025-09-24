"""Public API for dc43.

Exports minimal runtime helpers: versioning (SemVer), ODCS helpers,
validation utilities, and DQ protocol types.
"""

from .versioning import SemVer
from .components.data_quality import (
    DQClient,
    DQStatus,
    ValidationResult,
    apply_contract,
    evaluate_contract,
    odcs_type_name_from_spark,
    schema_snapshot,
    spark_type_name,
    validate_dataframe,
)
from .odcs import (
    BITOL_SCHEMA_URL,
    as_odcs_dict,
    ensure_version,
    contract_identity,
    list_properties,
    fingerprint,
    build_odcs,
    to_model,
)
try:
    # Convenience re-export of the official ODCS model
    from open_data_contract_standard.model import OpenDataContractStandard  # type: ignore
except Exception:  # pragma: no cover
    OpenDataContractStandard = None  # type: ignore


__all__ = [
    "SemVer",
    "ValidationResult",
    "evaluate_contract",
    "validate_dataframe",
    "schema_snapshot",
    "spark_type_name",
    "odcs_type_name_from_spark",
    "apply_contract",
    "BITOL_SCHEMA_URL",
    "as_odcs_dict",
    "ensure_version",
    "contract_identity",
    "list_properties",
    "fingerprint",
    "build_odcs",
    "to_model",
    "OpenDataContractStandard",
    "DQClient",
    "DQStatus",
]

__version__ = "0.1.0"
