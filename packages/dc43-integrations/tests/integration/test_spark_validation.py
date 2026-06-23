import pytest
from datetime import datetime

from open_data_contract_standard.model import (
    OpenDataContractStandard,
    SchemaObject,
    SchemaProperty,
    DataQuality,
    Description,
)

from dc43_integrations.spark.validation import apply_contract


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


def test_apply_contract_can_keep_extra_columns(spark):
    contract = make_contract()
    data = [
        (1, 101, datetime(2024, 1, 1, 10, 0, 0), 10.0, "EUR", "note"),
    ]
    df = spark.createDataFrame(
        data,
        ["order_id", "customer_id", "order_ts", "amount", "currency", "extra"],
    )
    out = apply_contract(df, contract, select_only_contract_columns=False)
    assert out.columns[:5] == ["order_id", "customer_id", "order_ts", "amount", "currency"]
    assert out.columns[-1] == "extra"
