import pytest

from open_data_contract_standard.model import (
    OpenDataContractStandard,
    SchemaObject,
    SchemaProperty,
    DataQuality,
    Description,
)

from dc43_integrations.spark.validation import apply_contract
from dc43_integrations.spark.data_quality import collect_observations
from dc43_service_clients.data_quality.client.local import LocalDataQualityServiceClient
from dc43_service_clients.data_quality import ObservationPayload
from datetime import datetime
from pyspark.sql.types import (
    StructType,
    StructField,
    LongType,
    TimestampType,
    DoubleType,
    StringType,
)


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


def test_validate_ok(spark):
    contract = make_contract()
    data = [
        (1, 101, datetime(2024, 1, 1, 10, 0, 0), 10.0, "EUR"),
        (2, 102, datetime(2024, 1, 2, 10, 0, 0), 20.5, "USD"),
    ]
    df = spark.createDataFrame(data, ["order_id", "customer_id", "order_ts", "amount", "currency"])
    client = LocalDataQualityServiceClient()
    expectations = client.describe_expectations(contract=contract)
    schema, metrics = collect_observations(
        df,
        contract,
        expectations=expectations,
    )
    res = client.evaluate(
        contract=contract,
        payload=ObservationPayload(metrics=metrics, schema=schema),
    )
    assert res.ok
    assert not res.errors
    assert res.metrics["row_count"] == 2
    assert "schema" in res.details
    assert res.schema["order_id"]["odcs_type"] == "bigint"


def test_validate_type_mismatch(spark):
    contract = make_contract()
    data = [
        (1, 101, datetime(2024, 1, 1, 10, 0, 0), "not-a-double", "EUR"),
    ]
    df = spark.createDataFrame(data, ["order_id", "customer_id", "order_ts", "amount", "currency"])
    client = LocalDataQualityServiceClient()
    expectations = client.describe_expectations(contract=contract)
    schema, metrics = collect_observations(
        df,
        contract,
        expectations=expectations,
    )
    res = client.evaluate(
        contract=contract,
        payload=ObservationPayload(metrics=metrics, schema=schema),
    )
    # amount is string but expected double, should report mismatch
    assert not res.ok
    assert any("type mismatch" in e for e in res.errors)
    assert res.metrics["row_count"] == 1


def test_validate_required_nulls(spark):
    contract = make_contract()
    data = [
        (1, None, datetime(2024, 1, 1, 10, 0, 0), 10.0, "EUR"),
    ]
    schema = StructType(
        [
            StructField("order_id", LongType(), False),
            StructField("customer_id", LongType(), True),
            StructField("order_ts", TimestampType(), True),
            StructField("amount", DoubleType(), True),
            StructField("currency", StringType(), True),
        ]
    )
    df = spark.createDataFrame(data, schema=schema)
    client = LocalDataQualityServiceClient()
    expectations = client.describe_expectations(contract=contract)
    schema_obs, metrics = collect_observations(
        df,
        contract,
        expectations=expectations,
    )
    res = client.evaluate(
        contract=contract,
        payload=ObservationPayload(metrics=metrics, schema=schema_obs),
    )
    assert not res.ok
    assert any("contains" in e and "null" in e for e in res.errors)
    assert res.schema["customer_id"]["nullable"]


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
