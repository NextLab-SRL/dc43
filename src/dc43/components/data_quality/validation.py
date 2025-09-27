"""Compatibility wrapper for contract-application helpers."""

from dc43.integration.data_quality import SPARK_TYPES, spark_type_name
from dc43.integration.validation import apply_contract

__all__ = ["apply_contract", "SPARK_TYPES", "spark_type_name"]
