from __future__ import annotations

from pathlib import Path

from open_data_contract_standard.model import (  # type: ignore
    Description,
    OpenDataContractStandard,
    SchemaObject,
    SchemaProperty,
    Server,
)

from dc43_integrations.spark.io import ContractVersionLocator, StaticDatasetLocator
from dc43_integrations.testing import generate_contract_dataset
from .helpers.orders import build_orders_contract


def test_generate_contract_dataset_materialises_storage(spark, tmp_path):
    contract = build_orders_contract(tmp_path / "orders", fmt="parquet")

    df, path = generate_contract_dataset(spark, contract, rows=5, seed=123)

    assert df.count() == 5
    assert set(df.columns) == {"order_id", "customer_id", "order_ts", "amount", "currency"}
    assert path == Path(tmp_path / "orders" / contract.version)
    assert any(path.iterdir())

    stored = spark.read.format("parquet").load(str(path))
    assert stored.count() == 5


def test_generate_contract_dataset_resolves_relative_paths_and_seed(spark, tmp_path):
    base_dir = tmp_path / "data"
    base_dir.mkdir()

    contract = OpenDataContractStandard(
        version="1.2.3",
        kind="DataContract",
        apiVersion="3.0.2",
        id="test.random",
        name="Random dataset",
        description=Description(usage="Generated dataset"),
        schema=[
            SchemaObject(
                name="random",
                properties=[
                    SchemaProperty(name="identifier", physicalType="bigint", required=True),
                    SchemaProperty(name="active", physicalType="boolean", required=True),
                    SchemaProperty(name="notes", physicalType="varchar(20)", required=False),
                ],
            )
        ],
        servers=[Server(server="local", type="filesystem", path="datasets/random", format="json")],
    )

    base_target = base_dir / "datasets" / "random"
    locator_one = ContractVersionLocator(
        dataset_version="v1.0.0/test",
        base=StaticDatasetLocator(path=str(base_target)),
    )
    locator_two = ContractVersionLocator(
        dataset_version="v1.0.0/test-second",
        base=StaticDatasetLocator(path=str(base_target)),
    )

    df_one, path_one = generate_contract_dataset(
        spark,
        contract,
        rows=3,
        dataset_locator=locator_one,
        seed=77,
    )
    df_two, path_two = generate_contract_dataset(
        spark,
        contract,
        rows=3,
        dataset_locator=locator_two,
        seed=77,
    )

    assert path_one.parent == path_two.parent == base_target
    assert path_one.name == "v1.0.0_test"
    assert path_two.name == "v1.0.0_test-second"

    assert [tuple(row) for row in df_one.collect()] == [tuple(row) for row in df_two.collect()]
    assert any(path_one.iterdir())
    assert any(path_two.iterdir())
