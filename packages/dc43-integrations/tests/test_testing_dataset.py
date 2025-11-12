import pytest

pytest.importorskip("faker", reason="faker is required for dataset testing helpers")

from open_data_contract_standard.model import (  # type: ignore
    Description,
    OpenDataContractStandard,
    SchemaObject,
    SchemaProperty,
)

from dc43_integrations.testing import generate_contract_dataset

from .helpers.orders import build_orders_contract


def test_generate_contract_dataset_returns_dataframe(spark, tmp_path):
    contract = build_orders_contract(tmp_path / "orders", fmt="parquet")

    df = generate_contract_dataset(spark, contract, rows=5, seed=123)

    assert df.count() == 5
    assert set(df.columns) == {"order_id", "customer_id", "order_ts", "amount", "currency"}


def test_generate_contract_dataset_respects_seed(spark, tmp_path):
    contract = build_orders_contract(tmp_path / "orders")

    df_one = generate_contract_dataset(spark, contract, rows=3, seed=77)
    df_two = generate_contract_dataset(spark, contract, rows=3, seed=77)

    assert [tuple(row) for row in df_one.collect()] == [tuple(row) for row in df_two.collect()]


def test_generate_contract_dataset_allows_schema_skip(spark, tmp_path):
    contract = build_orders_contract(tmp_path / "orders")

    df = generate_contract_dataset(spark, contract, rows=1, seed=1, validate_schema=False)

    assert df.count() == 1


def test_generate_contract_dataset_requires_properties(spark):
    contract = OpenDataContractStandard(
        version="1.0.0",
        kind="DataContract",
        apiVersion="3.0.2",
        id="empty.contract",
        name="Empty",
        description=Description(usage="No fields"),
        schema=[SchemaObject(name="empty", properties=[])],
    )

    with pytest.raises(ValueError, match="schema properties"):
        generate_contract_dataset(spark, contract, rows=1)
