"""ASGI entrypoint for serving dc43 backends over HTTP."""

from __future__ import annotations

import os
from pathlib import Path
from typing import Sequence

try:  # pragma: no cover - optional dependency guard for lightweight installs
    from fastapi import FastAPI
except ModuleNotFoundError as exc:  # pragma: no cover - raised when extras absent
    raise ModuleNotFoundError(
        "FastAPI is required to run the service backend web application. "
        "Install 'dc43-service-backends[http]' to enable this entrypoint."
    ) from exc

from .auth import bearer_token_dependency
from .contracts.backend.stores import FSContractStore
from .contracts.backend.stores.interface import ContractStore
from .web import build_local_app


def _resolve_store() -> ContractStore:
    """Return a contract store based on the ``DC43_CONTRACT_STORE`` variable."""

    root = os.getenv("DC43_CONTRACT_STORE")
    if root:
        path = Path(root)
    else:
        path = Path.cwd() / "contracts"
    path.mkdir(parents=True, exist_ok=True)
    return FSContractStore(str(path))


def _resolve_dependencies() -> Sequence[object] | None:
    """Return global router dependencies (authentication) if configured."""

    token = os.getenv("DC43_BACKEND_TOKEN")
    if token:
        return [bearer_token_dependency(token)]
    return None


def create_app() -> FastAPI:
    """Build a FastAPI application backed by local filesystem services."""

    store = _resolve_store()
    dependencies = _resolve_dependencies()
    return build_local_app(store, dependencies=dependencies)


# Module-level application so ``uvicorn dc43_service_backends.webapp:app`` works.
app = create_app()


__all__ = ["create_app", "app"]
