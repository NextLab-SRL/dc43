"""Backwards compatible shim to :mod:`dc43.lib.components.contract_validation`."""

from dc43.lib.components.contract_validation import (
    ExpectationSpec,
    ValidationResult,
    evaluate_contract,
    evaluate_observations,
    expectation_specs,
)

__all__ = [
    "ExpectationSpec",
    "ValidationResult",
    "evaluate_contract",
    "evaluate_observations",
    "expectation_specs",
]
