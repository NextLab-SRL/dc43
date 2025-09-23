"""Compatibility shim for DLT helpers."""

from dc43.components.integration.dlt_helpers import (
    apply_dlt_expectations,
    expectations_from_contract,
)

__all__ = ["apply_dlt_expectations", "expectations_from_contract"]
