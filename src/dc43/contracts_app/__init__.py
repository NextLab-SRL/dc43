"""Compatibility wrapper for the external contracts app package."""

from __future__ import annotations

from importlib import import_module
import sys
from types import ModuleType


def _load_server() -> ModuleType:
    """Import and return the contracts app server module."""

    module = import_module("dc43_contracts_app.server")
    sys.modules.setdefault(__name__ + ".server", module)
    return module


server = _load_server()
__all__ = ["server"]
