"""ASGI entrypoint for serving dc43 backends over HTTP."""

from __future__ import annotations

import os
from pathlib import Path
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
from .contracts.backend.stores import (
    CollibraContractStore,
    FSContractStore,
    HttpCollibraContractAdapter,
    StubCollibraContractAdapter,
)
from .contracts.backend.stores.interface import ContractStore
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


def _resolve_store(config: ServiceBackendsConfig) -> ContractStore:
    """Return a contract store instance derived from the active configuration."""

    store_config = config.contract_store
    store_type = (store_config.type or "filesystem").lower()

    if store_type == "filesystem":
        root = store_config.root
        path = Path(root) if root else Path.cwd() / "contracts"
        path.mkdir(parents=True, exist_ok=True)
        return FSContractStore(str(path))

    if store_type == "collibra_stub":
        base_path = store_config.base_path or store_config.root
        path = Path(base_path).expanduser() if base_path else None
        catalog = store_config.catalog or None
        adapter = StubCollibraContractAdapter(
            base_path=str(path) if path else None,
            catalog=catalog,
        )
        return CollibraContractStore(
            adapter,
            default_status=store_config.default_status,
            status_filter=store_config.status_filter,
        )

    if store_type == "collibra_http":
        if not store_config.base_url:
            raise RuntimeError(
                "contract_store.base_url is required when type is 'collibra_http'"
            )
        adapter = HttpCollibraContractAdapter(
            store_config.base_url,
            token=store_config.token,
            timeout=store_config.timeout,
            contract_catalog=store_config.catalog or None,
            contracts_endpoint_template=(
                store_config.contracts_endpoint_template
                or "/rest/2.0/dataproducts/{data_product}/ports/{port}/contracts"
            ),
        )
        return CollibraContractStore(
            adapter,
            default_status=store_config.default_status,
            status_filter=store_config.status_filter,
        )

    raise RuntimeError(f"Unsupported contract store type: {store_type}")


def _resolve_dependencies(config: ServiceBackendsConfig) -> Sequence[object] | None:
    """Return global router dependencies (authentication) if configured."""

    token = config.auth.token
    if token:
        return [bearer_token_dependency(token)]
    return None


def create_app(config: ServiceBackendsConfig | None = None) -> FastAPI:
    """Build a FastAPI application backed by local filesystem services."""

    active_config = configure_from_config(config)
    store = _resolve_store(active_config)
    dependencies = _resolve_dependencies(active_config)
    return build_local_app(store, dependencies=dependencies)


# Module-level application so ``uvicorn dc43_service_backends.webapp:app`` works.
app = create_app()


__all__ = ["create_app", "app", "configure_from_config"]
