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
    from .mock_collibra_service import MockCollibraService

    mock_service = MockCollibraService()
    
    # Pre-populate relation graph for the Port
    dp_uuid = "dp-uuid-sales"
    port_uuid = "port-uuid-gold"
    mock_service.add_asset(dp_uuid, "dp-sales", "Data Product", type_id=mock_service.data_product_type_id)
    mock_service.add_asset(port_uuid, "gold-port", "Port")
    mock_service.add_relation(
        source_id=dp_uuid,
        target_id=port_uuid,
        relation_type_name="Data Product contains Port",
        relation_type_id=mock_service.relation_type_contains_id
    )

    client = httpx.Client(transport=mock_service.get_transport(), base_url="https://collibra.example.com")
    contract_catalog = {"sales.orders": ("dp-sales", "gold-port")}

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


def test_http_gateway_cascading_lookup():
    httpx = pytest.importorskip("httpx")
    from .mock_collibra_service import MockCollibraService

    mock_service = MockCollibraService()
    
    # Pre-populate entire relation graph: DP -> Port -> Contract
    dp_uuid = "dp-uuid-sales"
    port_uuid = "port-uuid-gold"
    contract_uuid = "contract-uuid-sales-orders"
    
    mock_service.add_asset(dp_uuid, "dp-sales", "Data Product", type_id=mock_service.data_product_type_id)
    mock_service.add_asset(port_uuid, "gold-port", "Port")
    mock_service.add_asset(contract_uuid, "sales.orders", "Data Contract")
    
    mock_service.add_relation(
        source_id=dp_uuid,
        target_id=port_uuid,
        relation_type_name="Data Product contains Port",
        relation_type_id=mock_service.relation_type_contains_id
    )
    mock_service.add_relation(
        source_id=contract_uuid,
        target_id=port_uuid,
        relation_type_name="Port governed by Data Contract",
        relation_type_id=mock_service.relation_type_governed_id
    )

    # Manually configure the contract version inside mock service
    mock_service.contracts[contract_uuid] = {
        "id": contract_uuid,
        "manifestId": "sales.orders",
        "name": "sales.orders",
        "domainId": "domain-uuid-1",
        "domainName": "Mock Domain",
        "activeVersion": "1.0.0"
    }
    
    import yaml
    from dc43_service_backends.core.odcs import as_odcs_dict
    yaml_str = yaml.dump(as_odcs_dict(_sample_contract("1.0.0")))
    
    mock_service.contract_manifests[contract_uuid] = {"1.0.0": yaml_str}
    mock_service.contract_versions[contract_uuid] = {
        "1.0.0": {
            "version": "1.0.0",
            "active": True,
            "format": "ODCS",
            "createdBy": "user-uuid",
            "createdOn": 1476703764163,
            "lastModifiedBy": "user-uuid",
            "lastModifiedOn": 1476703764163
        }
    }

    client = httpx.Client(transport=mock_service.get_transport(), base_url="https://collibra.example.com")
    contract_catalog = {"sales.orders": ("dp-sales", "gold-port")}

    gateway = HttpCollibraContractAdapter(
        base_url="https://collibra.example.com",
        token="token",
        contract_catalog=contract_catalog,
        client=client,
    )

    # Disable fast path in mock to force cascading lookup!
    original_handle = mock_service.handle_request
    
    def custom_handle(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/rest/dataProduct/v1/dataContracts" and request.method == "GET":
            return httpx.Response(200, json={"items": [], "limit": 100, "nextCursor": None})
        return original_handle(request)
        
    mock_service.handle_request = custom_handle

    store = CollibraContractStore(gateway)
    resolved_uuid = gateway._resolve_contract_uuid("sales.orders")
    assert resolved_uuid == contract_uuid

    versions = store.list_versions("sales.orders")
    assert versions == ["1.0.0"]

    doc = store.get("sales.orders", "1.0.0")
    assert doc.id == "sales.orders"
