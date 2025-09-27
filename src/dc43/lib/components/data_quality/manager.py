"""Compatibility proxy to the service data-quality manager implementation."""

from dc43.services.data_quality.backend.manager import DataQualityManager
from dc43.services.data_quality.models import ObservationPayload
from dc43.services.data_quality.backend.engine import ValidationResult

__all__ = ["DataQualityManager", "ObservationPayload", "ValidationResult"]
