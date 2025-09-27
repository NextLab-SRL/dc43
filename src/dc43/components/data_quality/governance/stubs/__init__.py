"""Compatibility layer for governance stubs.

Runtime stubs now live under :mod:`dc43.services.governance.backend.stubs` and
are re-exported here for callers that still import from the library layout.
"""

from dc43.services.governance.backend.stubs import StubDQClient

__all__ = ["StubDQClient"]
