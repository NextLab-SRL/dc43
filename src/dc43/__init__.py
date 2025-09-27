"""Public API for dc43.

Exports minimal runtime helpers: versioning (SemVer), ODCS helpers,
validation utilities, and DQ protocol types.
"""

from .versioning import SemVer
from .components.data_quality import DataQualityManager, DQClient, DQStatus, ObservationPayload
from .components.data_quality.engine import (
    ExpectationSpec,
    ValidationResult,
    evaluate_contract,
    expectation_specs,
)
from .components.governance_service import (
    GovernanceServiceClient,
    QualityAssessment,
    QualityDraftContext,
    build_local_governance_service,
)
from .components.data_quality.integration import (
    SPARK_TYPES,
    attach_failed_expectations,
    build_metrics_payload,
    collect_observations,
    compute_metrics,
    odcs_type_name_from_spark,
    schema_snapshot,
    spark_type_name,
    validate_dataframe,
)
from .components.data_quality.validation import apply_contract
from .components.contract_drafter import draft_from_observations, draft_from_validation_result
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
    "DataQualityManager",
    "ObservationPayload",
    "QualityAssessment",
    "QualityDraftContext",
    "GovernanceServiceClient",
    "ValidationResult",
    "ExpectationSpec",
    "expectation_specs",
    "evaluate_contract",
    "validate_dataframe",
    "schema_snapshot",
    "spark_type_name",
    "odcs_type_name_from_spark",
    "apply_contract",
    "SPARK_TYPES",
    "attach_failed_expectations",
    "collect_observations",
    "compute_metrics",
    "build_metrics_payload",
    "build_local_governance_service",
    "draft_from_observations",
    "draft_from_validation_result",
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
