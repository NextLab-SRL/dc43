from unittest.mock import MagicMock
import pytest
from open_data_contract_standard.model import (
    CustomProperty,
    Description,
    OpenDataContractStandard,
    SchemaObject,
    SchemaProperty,
)

from dc43_integrations.spark.ddl import ContractDDLBuilder


def make_test_contract(
    *,
    properties=None,
    custom_properties=None,
    description="Test table description",
):
    props = properties or [
        SchemaProperty(name="id", physicalType="bigint", required=True, primaryKey=True, description="Unique ID"),
        SchemaProperty(name="name", physicalType="string", required=True),
        SchemaProperty(name="created_date", physicalType="date", partitioned=True),
        SchemaProperty(name="amount", physicalType="double"),
    ]
    return OpenDataContractStandard(
        version="1.0.0",
        kind="DataContract",
        apiVersion="3.0.2",
        id="test.customer_orders",
        name="Customer Orders",
        description=Description(usage=description) if description else None,
        customProperties=custom_properties or [],
        schema=[
            SchemaObject(
                name="customer_orders",
                properties=props,
            )
        ],
    )


def test_ddl_builder_basic():
    contract = make_test_contract()
    builder = ContractDDLBuilder(
        contract=contract,
        table="my_catalog.my_schema.orders",
        format="delta",
    )
    sql = builder.build_create_table_sql()

    assert "CREATE TABLE IF NOT EXISTS `my_catalog`.`my_schema`.`orders`" in sql
    assert "`id` bigint NOT NULL COMMENT 'Unique ID'" in sql
    assert "`name` string NOT NULL" in sql
    assert "`created_date` date" in sql
    assert "`amount` double" in sql
    assert "CONSTRAINT pk_orders PRIMARY KEY (`id`)" in sql
    assert "USING delta" in sql
    assert "PARTITIONED BY (`created_date`)" in sql
    assert "COMMENT 'Test table description'" in sql


def test_ddl_builder_clustering():
    custom_props = [
        CustomProperty(property="clustering", value=["id", "name"]),
        CustomProperty(property="tableProperties", value={"delta.enableChangeDataFeed": "true"}),
    ]
    contract = make_test_contract(custom_properties=custom_props)
    builder = ContractDDLBuilder(
        contract=contract,
        table="orders",
        format="delta",
        table_properties={"custom.prop": "value123"},
    )
    sql = builder.build_create_table_sql()

    assert "CLUSTER BY (`id`, `name`)" in sql
    assert "PARTITIONED BY" not in sql
    assert "TBLPROPERTIES ('custom.prop' = 'value123', 'delta.enableChangeDataFeed' = 'true')" in sql


def test_ddl_builder_modifier():
    contract = make_test_contract()
    builder = ContractDDLBuilder(
        contract=contract,
        table="orders",
        format="delta",
        ddl_modifier=lambda sql: sql + "\n-- custom tweak",
    )
    sql = builder.build_create_table_sql()
    assert sql.endswith("-- custom tweak")


def test_ddl_builder_path_only():
    contract = make_test_contract()
    builder = ContractDDLBuilder(
        contract=contract,
        path="/mnt/data/orders",
        format="delta",
    )
    sql = builder.build_create_table_sql()
    assert "CREATE TABLE IF NOT EXISTS delta.`/mnt/data/orders`" in sql


def test_ddl_builder_table_and_path():
    contract = make_test_contract()
    builder = ContractDDLBuilder(
        contract=contract,
        table="orders",
        path="/mnt/data/orders",
        format="delta",
    )
    sql = builder.build_create_table_sql()
    assert "CREATE TABLE IF NOT EXISTS `orders`" in sql
    assert "LOCATION '/mnt/data/orders'" in sql


def test_ddl_builder_no_properties_raises():
    contract = OpenDataContractStandard(
        version="1.0.0",
        kind="DataContract",
        apiVersion="3.0.2",
        id="empty.contract",
        schema=[],
    )
    builder = ContractDDLBuilder(contract=contract, table="orders")
    with pytest.raises(ValueError, match="has no defined properties"):
        builder.build_create_table_sql()


def test_ddl_builder_execute():
    contract = make_test_contract()
    builder = ContractDDLBuilder(
        contract=contract,
        table="orders",
        format="delta",
    )
    mock_spark = MagicMock()
    builder.execute(mock_spark)
    mock_spark.sql.assert_called_once()


def test_ddl_builder_parquet_omits_pk_and_delta_clustering():
    custom_props = [
        CustomProperty(property="clustering", value=["id", "name"]),
        CustomProperty(property="tableProperties", value={"delta.enableChangeDataFeed": "true", "custom.flag": "active"}),
    ]
    contract = make_test_contract(custom_properties=custom_props)
    builder = ContractDDLBuilder(
        contract=contract,
        path="/mnt/data/orders_parquet",
        format="parquet",
    )
    sql = builder.build_create_table_sql()
    assert "CREATE TABLE IF NOT EXISTS `orders_parquet`" in sql
    assert "USING parquet" in sql
    assert "CONSTRAINT pk_orders_parquet PRIMARY KEY" not in sql
    assert "CLUSTER BY" not in sql
    assert "PARTITIONED BY (`created_date`)" in sql
    assert "LOCATION '/mnt/data/orders_parquet'" in sql
    assert "'custom.flag' = 'active'" in sql
    assert "delta.enableChangeDataFeed" not in sql

