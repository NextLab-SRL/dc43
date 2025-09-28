"""Shared data structures used across data-quality services."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Mapping, Optional


@dataclass(slots=True)
class ObservationPayload:
    """Container describing cached observations for a dataset evaluation."""

    metrics: Mapping[str, object]
    schema: Optional[Mapping[str, Mapping[str, object]]] = None
    reused: bool = False


__all__ = ["ObservationPayload"]
