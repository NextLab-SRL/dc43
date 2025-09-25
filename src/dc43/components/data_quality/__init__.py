"""Data-quality manager facade with lazy re-exports."""

from __future__ import annotations

from importlib import import_module
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:  # pragma: no cover - import cycle guard
    from .governance import DQClient, DQStatus  # noqa: F401
    from .integration import (  # noqa: F401
        attach_failed_expectations,
        validate_dataframe,
    )
    from .manager import DataQualityManager, QualityAssessment, QualityDraftContext  # noqa: F401
    from .validation import apply_contract  # noqa: F401

__all__ = [
    "DataQualityManager",
    "QualityAssessment",
    "QualityDraftContext",
    "DQClient",
    "DQStatus",
    "apply_contract",
    "validate_dataframe",
    "attach_failed_expectations",
]

_EXPORT_MAP = {
    "DQClient": ("governance", "DQClient"),
    "DQStatus": ("governance", "DQStatus"),
    "DataQualityManager": ("manager", "DataQualityManager"),
    "QualityAssessment": ("manager", "QualityAssessment"),
    "QualityDraftContext": ("manager", "QualityDraftContext"),
    "apply_contract": ("validation", "apply_contract"),
    "validate_dataframe": ("integration", "validate_dataframe"),
    "attach_failed_expectations": ("integration", "attach_failed_expectations"),
}


def __getattr__(name: str) -> Any:  # pragma: no cover - import indirection
    if name not in _EXPORT_MAP:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
    module_name, attr = _EXPORT_MAP[name]
    module = import_module(f".{module_name}", __name__)
    value = getattr(module, attr)
    globals()[name] = value
    return value


def __dir__() -> list[str]:  # pragma: no cover - helper for introspection
    return sorted(set(list(globals().keys()) + list(_EXPORT_MAP.keys())))
