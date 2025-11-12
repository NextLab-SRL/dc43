from open_data_contract_standard.model import OpenDataContractStandard  # type: ignore

import pytest

pytest.importorskip("faker", reason="faker is required for contract drafting dataset tests")

from dc43_integrations.spark.contracts import (
    DraftContractResult,
    draft_contract_from_dataframe,
)
from dc43_integrations.testing import generate_contract_dataset

from .helpers.orders import build_orders_contract


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
