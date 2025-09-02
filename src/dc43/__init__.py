"""Public API for dc43.

Exports minimal runtime helpers: versioning (SemVer), ODCS helpers,
validation utilities, and DQ protocol types.
"""

from .versioning import SemVer
from .integration.validation import (
    ValidationResult,
    validate_dataframe,
    apply_contract,
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
from .dq.base import DQClient, DQStatus

__all__ = [
    "SemVer",
    "ValidationResult",
    "validate_dataframe",
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
