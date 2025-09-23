"""Compatibility shim for the DQ interface.

Prefer importing :mod:`dc43.dq.interface` instead.
"""

from .interface import DQClient, DQStatus

__all__ = ["DQClient", "DQStatus"]
