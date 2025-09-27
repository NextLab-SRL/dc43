"""Compatibility exports for contract drafting helpers.

The drafting logic now lives under :mod:`dc43.services.contracts.backend`.
This module re-exports those helpers so library callers keep functioning while
migrations take place.
"""

from dc43.services.contracts.backend.drafting import (
    draft_from_observations,
    draft_from_validation_result,
)

__all__ = ["draft_from_observations", "draft_from_validation_result"]
