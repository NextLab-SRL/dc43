"""Data-quality specific utilities that are independent from service runtimes."""

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
