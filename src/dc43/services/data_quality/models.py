"""Shared data structures used across data-quality services."""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from dataclasses import InitVar, dataclass, field
from inspect import isdatadescriptor
from typing import Any, Dict, List, Optional


ValidationStatusState = tuple[str, ...]
_KNOWN_STATUSES: ValidationStatusState = ("ok", "warn", "block", "unknown")


@dataclass(slots=True)
class ObservationPayload:
    """Container describing cached observations for a dataset evaluation."""

    metrics: Mapping[str, object]
    schema: Optional[Mapping[str, Mapping[str, object]]] = None
    reused: bool = False


@dataclass
class ValidationResult:
    """Outcome produced by a data-quality evaluation or governance verdict."""

    ok: bool = True
    errors: List[str] = field(default_factory=list)
    warnings: List[str] = field(default_factory=list)
    metrics: Dict[str, Any] = field(default_factory=dict)
    schema: Dict[str, Dict[str, Any]] = field(default_factory=dict)
    status: str = "unknown"
    reason: Optional[str] = None
    details: InitVar[Optional[Mapping[str, Any]]] = None
    _details: Dict[str, Any] = field(default_factory=dict, init=False, repr=False)

    def __post_init__(self, details: object | None) -> None:
        if self.status not in _KNOWN_STATUSES:
            self.status = "unknown"
        if details is not None:
            self._details = _coerce_details(details)
        if self.errors and self.ok:
            self.ok = False
        if self.status == "block":
            self.ok = False
        elif self.status in {"ok", "warn"} and not self.errors:
            self.ok = True

    @property
    def details(self) -> Dict[str, Any]:
        """Structured representation combining validation observations."""

        payload: Dict[str, Any] = {
            "errors": list(self.errors),
            "warnings": list(self.warnings),
            "metrics": dict(self.metrics),
            "schema": dict(self.schema),
        }
        if self.reason:
            payload.setdefault("reason", self.reason)
        payload.setdefault("status", self.status)
        if self._details:
            payload.update(self._details)
        return payload

    @details.setter
    def details(self, value: object) -> None:
        self._details = _coerce_details(value)

    @classmethod
    def from_status(
        cls,
        status: str,
        *,
        reason: Optional[str] = None,
        details: Optional[Mapping[str, Any]] = None,
    ) -> "ValidationResult":
        """Build a validation payload that represents a governance verdict."""

        return cls(
            ok=status != "block",
            status=status if status in _KNOWN_STATUSES else "unknown",
            reason=reason,
            details=details,
        )

    def merge_details(self, extra: Mapping[str, Any]) -> None:
        """Add ``extra`` fields to the detail payload without clobbering state."""

        if not extra:
            return
        merged: Dict[str, Any] = dict(self._details)
        merged.update(extra)
        self._details = _coerce_details(merged)


def _coerce_details(raw: object) -> Dict[str, Any]:
    """Normalise arbitrary detail payloads into a dictionary."""

    if raw is None:
        return {}
    if isinstance(raw, Mapping):
        return dict(raw)
    if isdatadescriptor(raw):
        return {}
    if hasattr(raw, "__get__") and not hasattr(raw, "__iter__"):
        return {}

    items = getattr(raw, "items", None)
    if callable(items):
        try:
            return dict(items())
        except TypeError:
            return {}

    if isinstance(raw, Iterable):
        try:
            return dict(raw)
        except (TypeError, ValueError):
            return {}

    return {}


__all__ = ["ObservationPayload", "ValidationResult"]
