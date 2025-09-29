"""Compatibility shim for contract drafting helpers."""

from __future__ import annotations

from typing import Any

__all__ = ["draft_from_observations", "draft_from_validation_result"]


def draft_from_observations(*args: Any, **kwargs: Any) -> Any:
    from .backend.drafting import draft_from_observations as _impl

    return _impl(*args, **kwargs)


def draft_from_validation_result(*args: Any, **kwargs: Any) -> Any:
    from .backend.drafting import draft_from_validation_result as _impl

    return _impl(*args, **kwargs)
