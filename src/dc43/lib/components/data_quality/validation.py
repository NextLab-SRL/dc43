"""Compatibility wrapper for contract-application helpers."""

from dc43.services.data_quality.backend.integration import SPARK_TYPES, spark_type_name
from dc43.services.data_quality.backend.validation import apply_contract

__all__ = ["apply_contract", "SPARK_TYPES", "spark_type_name"]
