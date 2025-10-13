"""Backend contracts and stubs for the data-quality service."""

from .engine import ExpectationSpec, evaluate_contract, expectation_specs
from .interface import DataQualityServiceBackend
from .local import LocalDataQualityServiceBackend
from .manager import DataQualityManager
from .remote import RemoteDataQualityServiceBackend
from dc43_service_clients.data_quality import ObservationPayload, ValidationResult

__all__ = [
    "DataQualityServiceBackend",
    "LocalDataQualityServiceBackend",
    "RemoteDataQualityServiceBackend",
    "DataQualityManager",
    "ObservationPayload",
    "ValidationResult",
    "ExpectationSpec",
    "evaluate_contract",
    "expectation_specs",
]
