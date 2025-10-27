"""Client-facing service APIs and shared models for dc43."""

from __future__ import annotations

from importlib import import_module
from typing import Any

_LAZY_MODULES = {"contracts", "data_products", "data_quality", "governance", "odps"}
_LAZY_EXPORTS: dict[str, tuple[str, str]] = {
    "ServiceClientsSuite": ("bootstrap", "ServiceClientsSuite"),
    "load_governance_client": ("bootstrap", "load_governance_client"),
    "load_service_clients": ("bootstrap", "load_service_clients"),
}

__all__ = sorted(_LAZY_MODULES | set(_LAZY_EXPORTS))


def __getattr__(name: str) -> Any:
    if name in _LAZY_EXPORTS:
        module_name, attr_name = _LAZY_EXPORTS[name]
        module = import_module(f".{module_name}", __name__)
        value = getattr(module, attr_name)
        globals()[name] = value
        return value
    if name in _LAZY_MODULES:
        module = import_module(f".{name}", __name__)
        globals()[name] = module
        return module
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


def __dir__() -> list[str]:
    return sorted(set(globals()) | set(__all__))
