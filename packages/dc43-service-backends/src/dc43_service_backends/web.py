"""Helpers for constructing HTTP applications with local backends."""

from __future__ import annotations

try:  # pragma: no cover - optional dependency guard
    from fastapi import FastAPI
except ModuleNotFoundError as exc:  # pragma: no cover
    raise ModuleNotFoundError(
        "FastAPI is required to build HTTP applications. Install "
        "'dc43-service-backends[http]' to enable this helper."
    ) from exc

from typing import Sequence

from dc43_service_backends.contracts.backend import ContractServiceBackend, LocalContractServiceBackend
from dc43_service_backends.contracts.backend.stores import FSContractStore
from dc43_service_backends.contracts.backend.stores.interface import ContractStore
from dc43_service_backends.data_quality.backend import (
    DataQualityServiceBackend,
    LocalDataQualityServiceBackend,
)
from dc43_service_backends.governance.backend import GovernanceServiceBackend, LocalGovernanceServiceBackend

from .server import build_app


def build_local_app(
    store: ContractStore | str,
    *,
    contract_backend: ContractServiceBackend | None = None,
    dq_backend: DataQualityServiceBackend | None = None,
    governance_backend: GovernanceServiceBackend | None = None,
    dependencies: Sequence[object] | None = None,
) -> FastAPI:
    """Build a FastAPI application wired against in-process backends."""

    if isinstance(store, str):
        store = FSContractStore(store)

    if contract_backend is None:
        contract_backend = LocalContractServiceBackend(store)
    if dq_backend is None:
        dq_backend = LocalDataQualityServiceBackend()
    if governance_backend is None:
        governance_backend = LocalGovernanceServiceBackend(
            contract_client=contract_backend,
            dq_client=dq_backend,
            draft_store=store,
        )

    return build_app(
        contract_backend=contract_backend,
        dq_backend=dq_backend,
        governance_backend=governance_backend,
        dependencies=dependencies,
    )


__all__ = ["build_local_app"]
