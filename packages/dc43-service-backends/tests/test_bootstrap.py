from __future__ import annotations

import sys
from pathlib import Path

import pytest

from types import SimpleNamespace

from dc43_service_backends.bootstrap import (
    build_backends,
    build_contract_store,
    build_data_product_backend,
    build_data_quality_backend,
    build_governance_store,
)
from dc43_service_backends.config import (
    ContractStoreConfig,
    DataProductStoreConfig,
    DataQualityBackendConfig,
    GovernanceStoreConfig,
    ServiceBackendsConfig,
)
from dc43_service_backends.contracts.backend.stores import DeltaContractStore, FSContractStore
from dc43_service_backends.data_products import DeltaDataProductServiceBackend, LocalDataProductServiceBackend
from dc43_service_backends.data_quality.backend import (
    LocalDataQualityServiceBackend,
    RemoteDataQualityServiceBackend,
)
from dc43_service_backends.governance.backend import LocalGovernanceServiceBackend
from dc43_service_backends.governance.storage import InMemoryGovernanceStore
from dc43_service_backends.governance.storage.filesystem import FilesystemGovernanceStore


class _StubSparkResult:
    def collect(self) -> list[tuple]:
        return []

    def head(self, _: int) -> list[tuple]:
        return []


class _StubSparkSession:
    def __init__(self) -> None:
        self.calls: list[str] = []

    def sql(self, statement: str) -> _StubSparkResult:
        self.calls.append(statement)
        return _StubSparkResult()


class _StubSparkBuilder:
    def getOrCreate(self) -> _StubSparkSession:
        return _StubSparkSession()


class _StubSpark:
    builder = _StubSparkBuilder()


def test_build_contract_store_filesystem(tmp_path: Path) -> None:
    cfg = ContractStoreConfig(type="filesystem", root=tmp_path)
    store = build_contract_store(cfg)
    assert isinstance(store, FSContractStore)


def test_build_contract_store_sql(tmp_path: Path) -> None:
    pytest.importorskip("sqlalchemy")
    dsn = f"sqlite:///{tmp_path / 'contracts.db'}"
    cfg = ContractStoreConfig(type="sql", dsn=dsn, table="contracts")

    store = build_contract_store(cfg)

    from dc43_service_backends.contracts.backend.stores.sql import SQLContractStore

    assert isinstance(store, SQLContractStore)


def test_build_data_product_backend_memory() -> None:
    backend = build_data_product_backend(DataProductStoreConfig(type="memory"))
    assert isinstance(backend, LocalDataProductServiceBackend)


def test_build_backends_delta(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr("dc43_service_backends.bootstrap.SparkSession", _StubSpark)
    config = ServiceBackendsConfig(
        contract_store=ContractStoreConfig(type="delta", table="unity.contracts"),
        data_product_store=DataProductStoreConfig(type="delta", table="unity.products"),
    )

    suite = build_backends(config)
    assert isinstance(suite.contract._store, DeltaContractStore)
    assert isinstance(suite.data_product, DeltaDataProductServiceBackend)
    assert isinstance(suite.data_quality, LocalDataQualityServiceBackend)
    assert isinstance(suite.governance_store, InMemoryGovernanceStore)
    assert isinstance(suite.governance, LocalGovernanceServiceBackend)
    assert isinstance(suite.contract_store, DeltaContractStore)
    assert isinstance(suite.link_hooks, tuple)


def test_build_data_quality_backend_local() -> None:
    backend = build_data_quality_backend(DataQualityBackendConfig(type="local"))
    assert isinstance(backend, LocalDataQualityServiceBackend)


def test_build_data_quality_backend_http(monkeypatch: pytest.MonkeyPatch) -> None:
    called: dict[str, object] = {}

    class _StubClient:
        def __init__(self, **kwargs: object) -> None:
            called.update(kwargs)

        def evaluate(self, **_: object) -> object:  # pragma: no cover - exercised via interface
            return object()

        def describe_expectations(self, **_: object) -> list[dict[str, object]]:
            return []

    stub_module = SimpleNamespace(RemoteDataQualityServiceClient=_StubClient)
    monkeypatch.setitem(sys.modules, "dc43_service_clients.data_quality.client.remote", stub_module)

    backend = build_data_quality_backend(
        DataQualityBackendConfig(
            type="http",
            base_url="https://observability.local",
            token="secret",
            token_header="X-Token",
            token_scheme="",
            headers={"X-Correlation-ID": "abc"},
        )
    )

    assert isinstance(backend, RemoteDataQualityServiceBackend)
    assert called["base_url"] == "https://observability.local"
    assert called["token"] == "secret"
    assert called["token_header"] == "X-Token"
    assert called["token_scheme"] == ""
    assert called["headers"] == {"X-Correlation-ID": "abc"}


def test_build_governance_store_memory() -> None:
    store = build_governance_store(GovernanceStoreConfig(type="memory"))
    assert isinstance(store, InMemoryGovernanceStore)


def test_build_governance_store_filesystem(tmp_path: Path) -> None:
    cfg = GovernanceStoreConfig(type="filesystem", root=tmp_path)
    store = build_governance_store(cfg)
    assert isinstance(store, FilesystemGovernanceStore)
    assert (tmp_path / "status").exists()
