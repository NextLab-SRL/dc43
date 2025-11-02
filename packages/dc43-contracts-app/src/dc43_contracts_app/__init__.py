"""Contract management web application components for DC43.

The portal exposes a reasonably large surface area (application factory,
configuration helpers, workspace utilities, etc.).  Importing everything at
module import time makes ``dc43_contracts_app`` very eager and can lead to
``ImportError`` cascades if an optional dependency is missing.  To keep the
package lightweight for callers that only need a subset of helpers, the module
now lazily resolves the underlying implementations on first attribute access.

This approach also ensures ``from dc43_contracts_app import configure_from_config``
continues to work even though the implementation lives in ``server``—the
attribute lookup triggers the deferred import and caches the result in the
module globals.
"""

from __future__ import annotations

from importlib import import_module
from typing import Any, Dict, Iterable

_CONFIG_EXPORTS = {
    "BackendConfig",
    "BackendProcessConfig",
    "ContractsAppConfig",
    "DocsChatConfig",
    "WorkspaceConfig",
    "load_config",
}

_SERVER_EXPORTS = {
    "DatasetRecord",
    "app",
    "configure_backend",
    "configure_from_config",
    "create_app",
    "load_records",
    "queue_flash",
    "scenario_run_rows",
}

__all__ = sorted({*_CONFIG_EXPORTS, *_SERVER_EXPORTS})


def _load_from(module_name: str, names: Iterable[str]) -> Dict[str, Any]:
    module = import_module(module_name, package=__name__)
    resolved: Dict[str, Any] = {}
    for attr in names:
        resolved[attr] = getattr(module, attr)
    return resolved


def __getattr__(name: str) -> Any:
    if name in _CONFIG_EXPORTS:
        resolved = _load_from(".config", [name])
    elif name in _SERVER_EXPORTS:
        resolved = _load_from(".server", [name])
    else:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")

    globals().update(resolved)
    return resolved[name]


def __dir__() -> list[str]:
    return sorted(set(globals()) | set(__all__))
