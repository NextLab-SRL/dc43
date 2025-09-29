from __future__ import annotations

import json
from datetime import datetime

import pytest

from dc43_service_backends.contracts.backend.stores import (
    HttpCollibraContractAdapter,
    StubCollibraContractAdapter,
)
from dc43_service_backends.contracts.backend.stores.collibra import CollibraContractStore
from open_data_contract_standard.model import (
    OpenDataContractStandard,
    SchemaObject,
    SchemaProperty,
    Server,
)  # type: ignore


def _sample_contract(version: str = "1.0.0") -> OpenDataContractStandard:
    return OpenDataContractStandard(
        version=version,
        kind="DatasetContract",
        apiVersion="3.0.2",
        id="sales.orders",
        name="Sales Orders",
        schema=[
            SchemaObject(
                name="orders",
                properties=[
                    SchemaProperty(name="order_id", physicalType="integer", required=True),
                    SchemaProperty(name="order_ts", physicalType="string"),
                ],
            )
        ],
        servers=[
            Server(server="s3", type="s3", path="datalake/orders", format="delta")
        ],
    )


def test_stub_gateway_roundtrip():
    gateway = StubCollibraContractAdapter()
    store = CollibraContractStore(gateway)

    contract = _sample_contract("1.0.0")
    store.put(contract)

    assert store.list_contracts() == ["sales.orders"]
    assert store.list_versions("sales.orders") == ["1.0.0"]

    retrieved = store.get("sales.orders", "1.0.0")
    assert retrieved.id == "sales.orders"
    assert retrieved.version == "1.0.0"
    assert retrieved.schema_[0].properties[0].name == "order_id"

    # Promote to validated and ensure status-filtered view behaves as expected
    gateway.update_status("sales.orders", "1.0.0", "Validated")
    validated_store = CollibraContractStore(gateway, status_filter="Validated")
    latest = validated_store.latest("sales.orders")
    assert latest is not None
    assert latest.version == "1.0.0"


def test_stub_gateway_validated_lookup():
    gateway = StubCollibraContractAdapter()
    contract = _sample_contract("1.0.0")
    gateway.submit_draft(contract)
    gateway.update_status("sales.orders", "1.0.0", "Validated")

    resolved = gateway.get_validated_contract("sales.orders")
    assert resolved["id"] == "sales.orders"

    newer = _sample_contract("1.1.0")
    gateway.submit_draft(newer)
    gateway.update_status("sales.orders", "1.1.0", "Validated")

    resolved = gateway.get_validated_contract("sales.orders")
    assert resolved["version"] == "1.1.0"


def test_http_gateway_with_mock_transport():
    httpx = pytest.importorskip("httpx")

    contract_catalog = {"sales.orders": ("dp-sales", "gold-port")}
    stored: dict[str, dict[str, dict[str, object]]] = {"sales.orders": {}}

    def handler(request: httpx.Request) -> httpx.Response:  # type: ignore[name-defined]
        path = request.url.path
        if path.endswith("/contracts") and request.method == "GET":
            versions = [
                {
                    "version": version,
                    "status": entry.get("status", "Draft"),
                    "updatedAt": entry.get("updatedAt", datetime.utcnow().isoformat()),
                }
                for version, entry in stored["sales.orders"].items()
            ]
            return httpx.Response(200, json={"data": versions})

        if path.endswith("/contracts/1.0.0") and request.method == "PUT":
            payload = json.loads(request.content.decode("utf-8"))
            stored["sales.orders"]["1.0.0"] = {
                "contract": payload["contract"],
                "status": payload.get("status", "Draft"),
            }
            return httpx.Response(204)

        if path.endswith("/contracts/1.0.0") and request.method == "GET":
            entry = stored["sales.orders"]["1.0.0"]
            return httpx.Response(200, json={"contract": entry["contract"], "status": entry.get("status")})

        return httpx.Response(404)

    transport = httpx.MockTransport(handler)  # type: ignore[attr-defined]
    client = httpx.Client(transport=transport, base_url="https://collibra.example.com")

    gateway = HttpCollibraContractAdapter(
        base_url="https://collibra.example.com",
        token="token",
        contract_catalog=contract_catalog,
        client=client,
    )

    store = CollibraContractStore(gateway)
    store.put(_sample_contract())

    versions = store.list_versions("sales.orders")
    assert versions == ["1.0.0"]

    doc = store.get("sales.orders", "1.0.0")
    assert doc.servers[0].path == "datalake/orders"

