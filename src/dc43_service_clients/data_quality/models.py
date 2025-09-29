"""Shared data structures used across data-quality services."""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from inspect import isdatadescriptor
from typing import Any, Mapping, Optional


ValidationStatusState = tuple[str, ...]
_KNOWN_STATUSES: ValidationStatusState = ("ok", "warn", "block", "unknown")


@dataclass(slots=True)
class ObservationPayload:
    """Container describing cached observations for a dataset evaluation."""

    metrics: Mapping[str, object]
    schema: Optional[Mapping[str, Mapping[str, object]]] = None
    reused: bool = False


class ValidationResult:
    """Outcome produced by a data-quality evaluation or governance verdict."""

    __slots__ = (
        "ok",
        "errors",
        "warnings",
        "metrics",
        "schema",
        "status",
        "reason",
        "_details",
    )

    def __init__(
        self,
        ok: bool = True,
        errors: Optional[Iterable[str]] = None,
        warnings: Optional[Iterable[str]] = None,
        metrics: Optional[Mapping[str, Any]] = None,
        schema: Optional[Mapping[str, Mapping[str, Any]]] = None,
        *,
        status: str = "unknown",
        reason: Optional[str] = None,
        details: object | None = None,
    ) -> None:
        self.ok = bool(ok)
        self.errors = list(errors or [])
        self.warnings = list(warnings or [])
        self.metrics = dict(metrics or {})
        self.schema = {key: dict(value) for key, value in (schema or {}).items()}
        self.status = status if status in _KNOWN_STATUSES else "unknown"
        self.reason = reason
        self._details = coerce_details(details)
        if self.errors and self.ok:
            self.ok = False
        if self.status == "block":
            self.ok = False
        elif self.status in {"ok", "warn"} and not self.errors:
            self.ok = True

    def _build_details_payload(self) -> dict[str, Any]:
        payload: dict[str, Any] = {
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

    @property
    def details(self) -> dict[str, Any]:
        """Structured representation combining validation observations."""

        return self._build_details_payload()

    @details.setter
    def details(self, value: object) -> None:
        self._details = coerce_details(value)

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
        merged: dict[str, Any] = dict(self._details)
        merged.update(extra)
        self._details = coerce_details(merged)


def coerce_details(raw: object) -> dict[str, Any]:
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


__all__ = ["ObservationPayload", "ValidationResult", "coerce_details"]
