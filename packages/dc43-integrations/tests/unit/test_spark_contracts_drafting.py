from open_data_contract_standard.model import (  # type: ignore
    OpenDataContractStandard,
    SchemaObject,
    SchemaProperty,
)

import pytest

pytest.importorskip("faker", reason="faker is required for contract drafting dataset tests")

from dc43_integrations.spark.contracts import (
    DraftContractResult,
    MutableStructType,
    dataframe_schema_from_contract,
    draft_contract_from_dataframe,
)
from dc43_integrations.testing import generate_contract_dataset

from ..helpers.orders import build_orders_contract


def test_draft_contract_from_dataframe_uses_existing_contract(spark, tmp_path):
    contract = build_orders_contract(tmp_path / "orders")
    df = generate_contract_dataset(spark, contract, rows=3, seed=99)

    result = draft_contract_from_dataframe(
        df,
        base_contract=contract,
        dataset_id="analytics.orders",
        dataset_version="2024-06-01",
    )

    assert isinstance(result, DraftContractResult)
    assert result.contract.id == contract.id
    assert result.contract.status == "draft"
    assert result.contract.version.startswith("0.1.1-")
    assert set(result.schema) == set(df.columns)


def test_draft_contract_from_dataframe_builds_base_contract(spark):
    df = spark.createDataFrame(
        [(1, "Alice"), (2, "Bob")],
        ["identifier", "name"],
    )

    result = draft_contract_from_dataframe(
        df,
        contract_id="generated.contract",
        name="Generated",
        base_version="0.0.1",
    )

    assert result.contract.id == "generated.contract"
    assert result.contract.status == "draft"
    assert set(result.schema) == {"identifier", "name"}


def test_draft_contract_from_dataframe_collects_metrics(spark, tmp_path):
    contract = build_orders_contract(tmp_path / "orders")
    df = generate_contract_dataset(spark, contract, rows=4, seed=12)

    result = draft_contract_from_dataframe(
        df,
        base_contract=contract,
        collect_metrics=True,
    )

    assert result.metrics.get("row_count") == 4


def test_dataframe_schema_from_contract_prefers_physical_type():
    from pyspark.sql.types import LongType, StringType

    contract = OpenDataContractStandard(
        version="0.1.0",
        kind="DataContract",
        apiVersion="3.0.2",
        id="test.types",
        name="Types",
        schema=[
            SchemaObject(
                name="types",
                properties=[
                    # Both physicalType and logicalType are defined: should use physicalType (bigint -> LongType)
                    SchemaProperty(name="col_both", physicalType="bigint", logicalType="string"),
                    # Only logicalType is defined: should fall back to logicalType (string -> StringType)
                    SchemaProperty(name="col_logical", logicalType="string"),
                    # Only physicalType is defined: should use physicalType (bigint -> LongType)
                    SchemaProperty(name="col_physical", physicalType="bigint"),
                ],
            )
        ],
    )

    spark_schema = dataframe_schema_from_contract(contract)

    # Check mapping
    fields = {f.name: f.dataType for f in spark_schema.fields}
    assert isinstance(fields["col_both"], LongType)
    assert isinstance(fields["col_logical"], StringType)
    assert isinstance(fields["col_physical"], LongType)


def test_dataframe_schema_from_contract_mutable_struct(spark):
    from pyspark.sql.types import StructField, StringType
    from pyspark.sql.functions import from_csv, col

    contract = OpenDataContractStandard(
        version="0.1.0",
        kind="DataContract",
        apiVersion="3.0.2",
        id="test.mutable",
        name="Mutable",
        schema=[
            SchemaObject(
                name="mutable",
                properties=[
                    SchemaProperty(name="id", physicalType="bigint"),
                    SchemaProperty(name="name", physicalType="string"),
                    SchemaProperty(name="score", physicalType="double"),
                ],
            )
        ],
    )

    # Generate mutable schema from contract
    schema = dataframe_schema_from_contract(contract)
    assert isinstance(schema, MutableStructType)

    # Test drop
    dropped = schema.drop("score")
    assert isinstance(dropped, MutableStructType)
    assert [f.name for f in dropped.fields] == ["id", "name"]

    # Test keep_only
    kept = schema.keep_only("id")
    assert isinstance(kept, MutableStructType)
    assert [f.name for f in kept.fields] == ["id"]

    # Test rename
    renamed = schema.rename({"name": "full_name"})
    assert isinstance(renamed, MutableStructType)
    assert [f.name for f in renamed.fields] == ["id", "full_name", "score"]

    # Test update_type
    updated = schema.update_type("id", StringType())
    assert isinstance(updated, MutableStructType)
    assert isinstance(updated.fields[0].dataType, StringType)

    # Test add_field
    added = schema.add_field(StructField("created_at", StringType(), True))
    assert isinstance(added, MutableStructType)
    assert [f.name for f in added.fields] == ["id", "name", "score", "created_at"]

    # Test Spark integration (DataFrame creation & from_csv)
    # A CSV dataset parsing scenario
    # Let's create a DataFrame with CSV strings
    df = spark.createDataFrame(
        [("1,Alice,95.5",), ("2,Bob,88.0",)],
        ["csv_data"],
    )

    # We drop 'score' using drop and parse the CSV string using from_csv with the dropped schema
    csv_schema = schema.drop("score")
    parsed_df = df.select(from_csv(col("csv_data"), csv_schema.simpleString()).alias("parsed"))
    
    # Check that it parsed id and name, and did not try to parse score
    rows = parsed_df.select("parsed.id", "parsed.name").collect()
    assert rows[0].id == 1
    assert rows[0].name == "Alice"
    assert rows[1].id == 2
    assert rows[1].name == "Bob"
