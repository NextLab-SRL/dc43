"""Convenience helpers for wiring service clients from configuration."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Optional

from dc43_service_backends.bootstrap import BackendSuite, build_backends
from dc43_service_backends.config import ServiceBackendsConfig, load_config
from dc43_service_clients.contracts import ContractServiceClient, LocalContractServiceClient
from dc43_service_clients.data_products import (
    DataProductServiceClient,
    LocalDataProductServiceClient,
)
from dc43_service_clients.data_quality import (
    DataQualityServiceClient,
    LocalDataQualityServiceClient,
)
from dc43_service_clients.governance import (
    GovernanceServiceClient,
    LocalGovernanceServiceClient,
    RemoteGovernanceServiceClient,
)


@dataclass(slots=True)
class ServiceClientsSuite:
    """Bundle of governance-facing service clients provisioned from config."""

    config: ServiceBackendsConfig
    governance: GovernanceServiceClient
    contract: Optional[ContractServiceClient] = None
    data_quality: Optional[DataQualityServiceClient] = None
    data_product: Optional[DataProductServiceClient] = None
    backends: Optional[BackendSuite] = None


def _should_use_remote_governance(config: ServiceBackendsConfig) -> bool:
    store = config.governance_store
    return (store.type or "").lower() in {"http", "remote"} and bool(store.base_url)


def load_service_clients(
    config: ServiceBackendsConfig | str | Path | None = None,
    *,
    include_data_product: bool = True,
) -> ServiceClientsSuite:
    """Load service clients using ``config`` or default configuration files."""

    if not isinstance(config, ServiceBackendsConfig):
        config = load_config(config)

    if _should_use_remote_governance(config):
        store = config.governance_store
        governance = RemoteGovernanceServiceClient(
            base_url=str(store.base_url),
            token=store.token,
            token_header=store.token_header,
            token_scheme=store.token_scheme,
            headers=store.headers or None,
        )
        return ServiceClientsSuite(config=config, governance=governance)

    backends = build_backends(config)
    governance = LocalGovernanceServiceClient(backends.governance)
    contract_client = LocalContractServiceClient(backends.contract)
    dq_client = LocalDataQualityServiceClient(backends.data_quality)
    data_product_client: Optional[DataProductServiceClient] = None
    if include_data_product:
        data_product_client = LocalDataProductServiceClient(backends.data_product)

    return ServiceClientsSuite(
        config=config,
        governance=governance,
        contract=contract_client,
        data_quality=dq_client,
        data_product=data_product_client,
        backends=backends,
    )


def load_governance_client(
    config: ServiceBackendsConfig | str | Path | None = None,
) -> GovernanceServiceClient:
    """Return a governance client initialised from ``config``."""

    suite = load_service_clients(config)
    return suite.governance


__all__ = [
    "ServiceClientsSuite",
    "load_service_clients",
    "load_governance_client",
]

