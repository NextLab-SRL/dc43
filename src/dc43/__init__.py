"""Top-level package for the dc43 platform.

The project is organised into dedicated sub-packages that make the layering
explicit:

* :mod:`dc43.components` provides the pure Python building blocks that can be
  embedded without any running services.
* :mod:`dc43.integration` collects runtime adapters used from execution
  environments such as Spark or Delta Live Tables.
* :mod:`dc43.clients` exposes the thin client abstractions used to talk to
  remote or in-process services.
* :mod:`dc43.services` contains service runtime helpers and local
  implementations.

The symbols continue to be re-exported here for backwards compatibility so
existing imports remain valid.
"""

from __future__ import annotations

from typing import Set

from . import clients, components, integration, services

# Re-export the public API of the dedicated sub-packages.
from .clients import *  # noqa: F401,F403
from .components import *  # noqa: F401,F403
from .integration import *  # noqa: F401,F403
from .services import *  # noqa: F401,F403

__version__ = "0.1.0"


def _collect_public_symbols() -> Set[str]:
    exported: Set[str] = set()
    for module in (components, integration, clients, services):
        exported.update(getattr(module, "__all__", ()))
    return exported


__all__ = sorted(_collect_public_symbols())
