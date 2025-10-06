from __future__ import annotations

from pathlib import Path

import pytest

from dc43_service_backends.bootstrap import (
    build_backends,
    build_contract_store,
    build_data_product_backend,
)
from dc43_service_backends.config import (
    ContractStoreConfig,
    DataProductStoreConfig,
    ServiceBackendsConfig,
)
from dc43_service_backends.contracts.backend.stores import DeltaContractStore, FSContractStore
from dc43_service_backends.data_products import DeltaDataProductServiceBackend, LocalDataProductServiceBackend


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

    contract_backend, data_product_backend = build_backends(config)
    assert isinstance(contract_backend._store, DeltaContractStore)
    assert isinstance(data_product_backend, DeltaDataProductServiceBackend)
