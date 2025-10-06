from __future__ import annotations

import json
from pathlib import Path

import pytest

pytest.importorskip("sqlalchemy")

from open_data_contract_standard.model import Description, OpenDataContractStandard, SchemaObject, SchemaProperty  # type: ignore

from dc43_service_backends.contracts.backend.stores.sql import SQLContractStore


def _build_contract(version: str = "1.0.0", description: str = "Orders facts") -> OpenDataContractStandard:
    return OpenDataContractStandard(
        version=version,
        kind="DataContract",
        apiVersion="3.0.2",
        id="test.orders",
        name="Orders",
        description=Description(usage=description),
        schema=[
            SchemaObject(
                name="orders",
                properties=[
                    SchemaProperty(name="order_id", physicalType="bigint", required=True),
                ],
            )
        ],
    )


@pytest.fixture()
def sql_engine(tmp_path: Path):
    pytest.importorskip("sqlalchemy")
    from sqlalchemy import create_engine

    db_path = tmp_path / "contracts.db"
    engine = create_engine(f"sqlite:///{db_path}")
    yield engine
    engine.dispose()


def test_sql_contract_store_round_trip(sql_engine) -> None:
    store = SQLContractStore(sql_engine)
    contract = _build_contract()

    store.put(contract)

    retrieved = store.get("test.orders", "1.0.0")
    assert retrieved.id == "test.orders"
    assert retrieved.description
    assert retrieved.description.usage == "Orders facts"

    contracts = store.list_contracts()
    assert contracts == ["test.orders"]

    versions = store.list_versions("test.orders")
    assert versions == ["1.0.0"]


def test_sql_contract_store_updates_existing_version(sql_engine) -> None:
    store = SQLContractStore(sql_engine)
    original = _build_contract()
    updated = _build_contract(description="Orders facts v2")

    store.put(original)
    store.put(updated)

    latest = store.get("test.orders", "1.0.0")
    assert latest.description
    assert latest.description.usage == "Orders facts v2"


def test_sql_contract_store_serialises_contract(sql_engine) -> None:
    store = SQLContractStore(sql_engine)
    contract = _build_contract()

    store.put(contract)

    from sqlalchemy import text

    with sql_engine.begin() as conn:
        payload = conn.execute(
            text("SELECT json FROM contracts WHERE contract_id = :cid AND version = :version"),
            {"cid": "test.orders", "version": "1.0.0"},
        ).scalar_one()

    decoded = json.loads(payload)
    assert decoded["id"] == "test.orders"
    assert decoded["version"] == "1.0.0"
