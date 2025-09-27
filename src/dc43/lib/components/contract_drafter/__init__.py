"""Helpers that turn runtime feedback into ODCS drafts."""

from .observations import draft_from_observations, draft_from_validation_result

__all__ = ["draft_from_observations", "draft_from_validation_result"]
