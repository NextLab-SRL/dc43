import pytest

from open_data_contract_standard.model import (
    OpenDataContractStandard,
    SchemaObject,
    SchemaProperty,
    Description,
)

from dc43.integration.validation import validate_dataframe, apply_contract
from datetime import datetime


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
                    SchemaProperty(name="order_id", physicalType="bigint", required=True, unique=True),
                    SchemaProperty(name="customer_id", physicalType="bigint", required=True),
                    SchemaProperty(name="order_ts", physicalType="timestamp", required=True),
                    SchemaProperty(name="amount", physicalType="double", required=True),
                    SchemaProperty(name="currency", physicalType="string", required=True, logicalTypeOptions={"enum": ["EUR", "USD"]}),
                ],
            )
        ],
    )


def test_validate_ok(spark):
    contract = make_contract()
    data = [
        (1, 101, datetime(2024, 1, 1, 10, 0, 0), 10.0, "EUR"),
        (2, 102, datetime(2024, 1, 2, 10, 0, 0), 20.5, "USD"),
    ]
    df = spark.createDataFrame(data, ["order_id", "customer_id", "order_ts", "amount", "currency"])
    res = validate_dataframe(df, contract)
    assert res.ok
    assert not res.errors


def test_validate_type_mismatch(spark):
    contract = make_contract()
    data = [
        (1, 101, datetime(2024, 1, 1, 10, 0, 0), "not-a-double", "EUR"),
    ]
    df = spark.createDataFrame(data, ["order_id", "customer_id", "order_ts", "amount", "currency"])
    res = validate_dataframe(df, contract)
    # amount is string but expected double, should report mismatch
    assert not res.ok
    assert any("type mismatch" in e for e in res.errors)


def test_apply_contract_aligns_and_casts(spark):
    contract = make_contract()
    # Intentionally shuffle and wrong types
    data = [("20.5", "USD", 2, 102, datetime(2024, 1, 2, 10, 0, 0))]
    df = spark.createDataFrame(data, ["amount", "currency", "order_id", "customer_id", "order_ts"])
    out = apply_contract(df, contract, auto_cast=True)
    # Ensure order and types are aligned
    assert out.columns == ["order_id", "customer_id", "order_ts", "amount", "currency"]
    dtypes = dict(out.dtypes)
    assert dtypes["order_id"] in ("bigint", "long")
    assert dtypes["amount"] in ("double",)
