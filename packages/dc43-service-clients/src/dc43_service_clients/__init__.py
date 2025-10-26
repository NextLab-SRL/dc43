"""Client-facing service APIs and shared models for dc43."""

from __future__ import annotations

from importlib import import_module
from typing import Any

from .bootstrap import ServiceClientsSuite, load_governance_client, load_service_clients

_LAZY_MODULES = {"contracts", "data_products", "data_quality", "governance", "odps"}
_DIRECT_EXPORTS = {
    "ServiceClientsSuite": ServiceClientsSuite,
    "load_governance_client": load_governance_client,
    "load_service_clients": load_service_clients,
}

__all__ = sorted(_LAZY_MODULES | set(_DIRECT_EXPORTS))


def __getattr__(name: str) -> Any:
    if name in _DIRECT_EXPORTS:
        value = _DIRECT_EXPORTS[name]
        globals()[name] = value
        return value
    if name in _LAZY_MODULES:
        module = import_module(f".{name}", __name__)
        globals()[name] = module
        return module
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


def __dir__() -> list[str]:
    return sorted(set(globals()) | set(__all__))
