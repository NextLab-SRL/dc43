import pytest


pytest.importorskip("dc43_service_backends")

from dc43_service_backends.config import GovernanceStoreConfig, ServiceBackendsConfig
from dc43_service_clients import load_governance_client, load_service_clients
from dc43_service_clients.governance import (
    LocalGovernanceServiceClient,
    RemoteGovernanceServiceClient,
)


def test_load_service_clients_local_defaults():
    config = ServiceBackendsConfig()
    suite = load_service_clients(config)

    assert isinstance(suite.governance, LocalGovernanceServiceClient)
    assert suite.contract is not None
    assert suite.data_quality is not None
    assert suite.backends is not None


def test_load_service_clients_remote_governance():
    config = ServiceBackendsConfig(
        governance_store=GovernanceStoreConfig(type="http", base_url="https://governance.example.com"),
    )
    suite = load_service_clients(config)

    assert isinstance(suite.governance, RemoteGovernanceServiceClient)
    assert suite.contract is None
    assert suite.data_quality is None

    client = load_governance_client(config)
    assert isinstance(client, RemoteGovernanceServiceClient)
