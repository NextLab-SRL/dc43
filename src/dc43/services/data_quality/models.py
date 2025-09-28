"""Shared data structures used across data-quality services."""

from __future__ import annotations

from dataclasses import InitVar, dataclass, field
from typing import Any, Dict, List, Mapping, Optional


DQStatusState = tuple[str, ...]
_KNOWN_STATUSES: DQStatusState = ("ok", "warn", "block", "unknown")


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

    def __post_init__(self, details: Optional[Mapping[str, Any]]) -> None:
        if self.status not in _KNOWN_STATUSES:
            self.status = "unknown"
        if details:
            self._details = dict(details)
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
    def details(self, value: Mapping[str, Any]) -> None:
        self._details = dict(value)

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


DQStatus = ValidationResult


__all__ = ["DQStatus", "ObservationPayload", "ValidationResult"]
