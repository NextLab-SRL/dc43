import pytest
from datetime import datetime

from open_data_contract_standard.model import (
    OpenDataContractStandard,
    SchemaObject,
    SchemaProperty,
    DataQuality,
    Description,
)

from dc43_service_clients.data_quality.client.local import LocalDataQualityServiceClient
from dc43_service_clients.data_quality import ObservationPayload

def make_contract():
    return OpenDataContractStandard(
        version="0.1.0",
        kind="DataContract",
        apiVersion="3.0.2",
        id="test.orders",
        name="Orders",
        description=Description(usage="Orders facts"),
        schema=[
            SchemaObject(
                name="orders",
                properties=[
                    SchemaProperty(name="order_id", physicalType="bigint", required=True),
                    SchemaProperty(name="customer_id", physicalType="bigint", required=True),
                    SchemaProperty(name="order_ts", physicalType="timestamp", required=True),
                    SchemaProperty(name="amount", physicalType="double", required=True),
                    SchemaProperty(
                        name="currency",
                        physicalType="string",
                        required=True,
                        quality=[DataQuality(rule="enum", mustBe=["EUR", "USD"])],
                    ),
                ],
            )
        ],
    )


def test_validate_ok():
    contract = make_contract()
    
    # Mock the physical schema that Spark would have gathered
    schema = {
        "order_id": {"odcs_type": "bigint", "nullable": False},
        "customer_id": {"odcs_type": "bigint", "nullable": False},
        "order_ts": {"odcs_type": "timestamp", "nullable": False},
        "amount": {"odcs_type": "double", "nullable": False},
        "currency": {"odcs_type": "string", "nullable": False},
    }
    
    # Mock the metrics Observation Collector would have found
    metrics = {
        "row_count": 2,
        "violations": {
            "order_id.not_null": 0,
            "customer_id.not_null": 0,
            "order_ts.not_null": 0,
            "amount.not_null": 0,
            "currency.not_null": 0,
            "currency.enum": 0,
        }
    }

    client = LocalDataQualityServiceClient()
    res = client.evaluate(
        contract=contract,
        payload=ObservationPayload(metrics=metrics, schema=schema),
    )
    assert res.ok
    assert not res.errors
    assert res.metrics["row_count"] == 2
    assert "schema" in res.details
    assert res.schema["order_id"]["odcs_type"] == "bigint"


def test_validate_type_mismatch():
    contract = make_contract()
    
    # Mock amount having the wrong type (string instead of double)
    schema = {
        "order_id": {"odcs_type": "bigint", "nullable": False},
        "customer_id": {"odcs_type": "bigint", "nullable": False},
        "order_ts": {"odcs_type": "timestamp", "nullable": False},
        "amount": {"odcs_type": "string", "nullable": False},
        "currency": {"odcs_type": "string", "nullable": False},
    }
    metrics = {"row_count": 1, "violations": {}}

    client = LocalDataQualityServiceClient()
    res = client.evaluate(
        contract=contract,
        payload=ObservationPayload(metrics=metrics, schema=schema),
    )
    
    # amount is string but expected double, should report mismatch
    assert not res.ok
    assert any("type mismatch" in e for e in res.errors)
    assert res.metrics["row_count"] == 1


def test_validate_required_nulls():
    contract = make_contract()
    
    # Mock schema reflecting that required fields are occasionally null
    schema = {
        "order_id": {"odcs_type": "bigint", "nullable": False},
        "customer_id": {"odcs_type": "bigint", "nullable": True},
        "order_ts": {"odcs_type": "timestamp", "nullable": True},
        "amount": {"odcs_type": "double", "nullable": True},
        "currency": {"odcs_type": "string", "nullable": True},
    }
    
    # Mock the observations indicating 1 null violation for customer_id
    metrics = {
        "row_count": 1, 
        "violations.not_null_customer_id": 1
    }

    client = LocalDataQualityServiceClient()
    res = client.evaluate(
        contract=contract,
        payload=ObservationPayload(metrics=metrics, schema=schema),
    )
    
    assert not res.ok
    assert any("contains" in e and "null" in e for e in res.errors)
    assert res.schema["customer_id"]["nullable"]
