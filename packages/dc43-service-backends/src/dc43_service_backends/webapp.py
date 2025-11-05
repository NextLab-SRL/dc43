"""ASGI entrypoint for serving dc43 backends over HTTP."""

from __future__ import annotations

import os
from typing import Sequence
from threading import Lock

try:  # pragma: no cover - optional dependency guard for lightweight installs
    from fastapi import FastAPI
except ModuleNotFoundError as exc:  # pragma: no cover - raised when extras absent
    raise ModuleNotFoundError(
        "FastAPI is required to run the service backend web application. "
        "Install 'dc43-service-backends[http]' to enable this entrypoint."
    ) from exc

from .auth import bearer_token_dependency
from .config import ServiceBackendsConfig, load_config
from .bootstrap import build_backends
from .web import build_local_app

_CONFIG_LOCK = Lock()
_ACTIVE_CONFIG: ServiceBackendsConfig | None = None


def configure_from_config(
    config: ServiceBackendsConfig | None = None,
) -> ServiceBackendsConfig:
    """Cache ``config`` and return the active backend configuration."""

    resolved = config or load_config()
    with _CONFIG_LOCK:
        global _ACTIVE_CONFIG
        _ACTIVE_CONFIG = resolved
    return resolved


def _current_config() -> ServiceBackendsConfig:
    with _CONFIG_LOCK:
        global _ACTIVE_CONFIG
        if _ACTIVE_CONFIG is None:
            _ACTIVE_CONFIG = load_config()
        return _ACTIVE_CONFIG


def _resolve_dependencies(config: ServiceBackendsConfig) -> Sequence[object] | None:
    """Return global router dependencies (authentication) if configured."""

    token = config.auth.token
    if token:
        return [bearer_token_dependency(token)]
    return None


def create_app(config: ServiceBackendsConfig | None = None) -> FastAPI:
    """Build a FastAPI application backed by local filesystem services."""

    active_config = configure_from_config(config)
    suite = build_backends(active_config)
    dependencies = _resolve_dependencies(active_config)
    contract_store = suite.contract_store
    if contract_store is None:  # pragma: no cover - defensive guard
        raise RuntimeError("build_backends did not expose a contract store")
    return build_local_app(
        contract_store,
        dependencies=dependencies,
        contract_backend=suite.contract,
        data_product_backend=suite.data_product,
        dq_backend=suite.data_quality,
        governance_backend=suite.governance,
        governance_store=suite.governance_store,
    )


# Module-level application so ``uvicorn dc43_service_backends.webapp:app`` works.
app = create_app()


__all__ = ["create_app", "app", "configure_from_config"]
