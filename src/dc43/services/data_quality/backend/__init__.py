"""Backend contracts and stubs for the data-quality service."""

from .engine import ExpectationSpec, evaluate_contract, expectation_specs
from .interface import DataQualityServiceBackend
from .local import LocalDataQualityServiceBackend
from .manager import DataQualityManager
from dc43.integration.spark.validation import apply_contract
from ..models import ObservationPayload, ValidationResult

__all__ = [
    "DataQualityServiceBackend",
    "LocalDataQualityServiceBackend",
    "DataQualityManager",
    "ObservationPayload",
    "ValidationResult",
    "ExpectationSpec",
    "evaluate_contract",
    "expectation_specs",
    "apply_contract",
]
