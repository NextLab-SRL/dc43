"""Data-quality engine primitives that evaluate observations."""

from .core import ValidationResult, evaluate_contract

__all__ = ["ValidationResult", "evaluate_contract"]
