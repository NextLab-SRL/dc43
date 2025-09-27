"""Compatibility exports for data-quality governance interfaces."""

from dc43.services.governance.backend.dq import DQClient, DQStatus

__all__ = ["DQClient", "DQStatus"]
