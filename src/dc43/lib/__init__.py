"""Core building blocks for using :mod:`dc43` as a standalone library.

The library surface purposely avoids depending on HTTP or service runtimes so
that callers can embed the data-quality engine, contract drafting helpers, and
Open Data Contract Standard utilities directly within their applications.
"""

from dc43.components.contract_drafter import (
    draft_from_observations,
    draft_from_validation_result,
)
from dc43.components.data_quality import (
    DataQualityManager,
    DQClient,
    DQStatus,
    apply_contract,
    attach_failed_expectations,
    validate_dataframe,
)
from dc43.components.data_quality.engine import (
    ExpectationSpec,
    ValidationResult,
    evaluate_contract,
    expectation_specs,
)
from dc43.components.data_quality.integration import (
    SPARK_TYPES,
    build_metrics_payload,
    collect_observations,
    compute_metrics,
    odcs_type_name_from_spark,
    schema_snapshot,
    spark_type_name,
)
from dc43.lib.data_quality import ObservationPayload
from dc43.odcs import (
    BITOL_SCHEMA_URL,
    as_odcs_dict,
    build_odcs,
    contract_identity,
    ensure_version,
    fingerprint,
    list_properties,
    to_model,
)
from dc43.versioning import SemVer

try:  # pragma: no cover - optional dependency for convenience re-export
    from open_data_contract_standard.model import OpenDataContractStandard  # type: ignore
except Exception:  # pragma: no cover
    OpenDataContractStandard = None  # type: ignore

__all__ = [
    "SemVer",
    "DataQualityManager",
    "DQClient",
    "DQStatus",
    "ObservationPayload",
    "ValidationResult",
    "ExpectationSpec",
    "expectation_specs",
    "evaluate_contract",
    "apply_contract",
    "validate_dataframe",
    "attach_failed_expectations",
    "collect_observations",
    "compute_metrics",
    "build_metrics_payload",
    "SPARK_TYPES",
    "spark_type_name",
    "odcs_type_name_from_spark",
    "schema_snapshot",
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
]
