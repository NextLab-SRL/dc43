"""Shared data structures used across data-quality services."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, List, Mapping, Optional


@dataclass
class DQStatus:
    """Status payload produced by data-quality governance checks."""

    status: str  # one of: ok, warn, block, unknown
    reason: Optional[str] = None
    details: Optional[Dict[str, Any]] = None


@dataclass(slots=True)
class ObservationPayload:
    """Container describing cached observations for a dataset evaluation."""

    metrics: Mapping[str, object]
    schema: Optional[Mapping[str, Mapping[str, object]]] = None
    reused: bool = False


@dataclass
class ValidationResult:
    """Outcome produced by a data-quality evaluation."""

    ok: bool
    errors: List[str]
    warnings: List[str]
    metrics: Dict[str, Any] = field(default_factory=dict)
    schema: Dict[str, Dict[str, Any]] = field(default_factory=dict)

    @property
    def details(self) -> Dict[str, Any]:
        """Structured representation combining validation observations."""

        return {
            "errors": self.errors,
            "warnings": self.warnings,
            "metrics": self.metrics,
            "schema": self.schema,
        }


__all__ = ["DQStatus", "ObservationPayload", "ValidationResult"]
