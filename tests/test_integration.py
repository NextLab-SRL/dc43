from pathlib import Path

import pytest

from open_data_contract_standard.model import (
    OpenDataContractStandard,
    SchemaObject,
    SchemaProperty,
    DataQuality,
    Description,
    Server,
)

from dc43.components.contract_store.impl.filesystem import FSContractStore
from dc43.components.integration.spark_io import (
    read_with_contract,
    write_with_contract,
    StaticDatasetLocator,
)
from dc43.components.integration.violation_strategy import SplitWriteViolationStrategy
from dc43.components.data_quality.governance.stubs import StubDQClient
from datetime import datetime
import logging


def make_contract(base_path: str, fmt: str = "parquet") -> OpenDataContractStandard:
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
        servers=[Server(server="local", type="filesystem", path=base_path, format=fmt)],
    )


def persist_contract(tmp_path: Path, contract: OpenDataContractStandard) -> FSContractStore:
    store = FSContractStore(str(tmp_path / "contracts"))
    store.put(contract)
    return store


def test_dq_integration_warn(spark, tmp_path: Path):
    data_dir = tmp_path / "parquet"
    contract = make_contract(str(data_dir))
    store = persist_contract(tmp_path, contract)
    # Prepare data with one enum violation for currency
    data = [
        (1, 101, datetime(2024, 1, 1, 10, 0, 0), 10.0, "EUR"),
        (2, 102, datetime(2024, 1, 2, 10, 0, 0), 20.5, "INR"),  # violation
    ]
    df = spark.createDataFrame(data, ["order_id", "customer_id", "order_ts", "amount", "currency"])
    df.write.mode("overwrite").format("parquet").save(str(data_dir))

    dq = StubDQClient(base_path=str(tmp_path / "dq_state"), block_on_violation=False)
    # enforce=False to avoid raising on validation expectation failures
    _, status = read_with_contract(
        spark,
        contract_id=contract.id,
        contract_store=store,
        expected_contract_version=f"=={contract.version}",
        enforce=False,
        dq_client=dq,
        return_status=True,
    )
    assert status is not None
    assert status.status in ("warn", "ok")


def test_write_validation_result_on_mismatch(spark, tmp_path: Path):
    dest_dir = tmp_path / "out"
    contract = make_contract(str(dest_dir))
    store = persist_contract(tmp_path, contract)
    # Missing required column 'currency' to trigger validation error
    data = [(1, 101, datetime(2024, 1, 1, 10, 0, 0), 10.0)]
    df = spark.createDataFrame(data, ["order_id", "customer_id", "order_ts", "amount"])
    result = write_with_contract(
        df=df,
        contract_id=contract.id,
        contract_store=store,
        expected_contract_version=f"=={contract.version}",
        mode="overwrite",
        enforce=False,  # continue writing despite mismatch
    )
    assert not result.ok
    assert result.errors
    assert any("currency" in err.lower() for err in result.errors)


def test_inferred_contract_id_simple(spark, tmp_path: Path):
    dest = tmp_path / "out" / "sample" / "1.0.0"
    df = spark.createDataFrame([(1,)], ["a"])
    # Without a contract the function simply writes the dataframe.
    result = write_with_contract(
        df=df,
        path=str(dest),
        format="parquet",
        mode="overwrite",
        enforce=False,
    )
    assert result.ok


def test_write_warn_on_path_mismatch(spark, tmp_path: Path):
    expected_dir = tmp_path / "expected"
    actual_dir = tmp_path / "actual"
    contract = make_contract(str(expected_dir))
    store = persist_contract(tmp_path, contract)
    data = [
        (1, 101, datetime(2024, 1, 1, 10, 0, 0), 10.0, "EUR"),
    ]
    df = spark.createDataFrame(
        data,
        ["order_id", "customer_id", "order_ts", "amount", "currency"],
    )
    result = write_with_contract(
        df=df,
        contract_id=contract.id,
        contract_store=store,
        expected_contract_version=f"=={contract.version}",
        path=str(actual_dir),
        mode="overwrite",
        enforce=False,
    )
    assert any("does not match" in w for w in result.warnings)


def test_write_path_version_under_contract_root(spark, tmp_path: Path, caplog):
    base_dir = tmp_path / "data"
    contract_path = base_dir / "orders_enriched.parquet"
    contract = make_contract(str(contract_path))
    store = persist_contract(tmp_path, contract)
    data = [
        (1, 101, datetime(2024, 1, 1, 10, 0, 0), 10.0, "EUR"),
    ]
    df = spark.createDataFrame(
        data,
        ["order_id", "customer_id", "order_ts", "amount", "currency"],
    )
    target = base_dir / "orders_enriched" / "1.0.0"
    with caplog.at_level(logging.WARNING):
        result = write_with_contract(
            df=df,
            contract_id=contract.id,
            contract_store=store,
            expected_contract_version=f"=={contract.version}",
            path=str(target),
            mode="overwrite",
            enforce=False,
        )
    assert not any("does not match contract server path" in msg for msg in caplog.messages)
    assert not any("does not match" in w for w in result.warnings)


def test_read_warn_on_format_mismatch(spark, tmp_path: Path, caplog):
    data_dir = tmp_path / "json"
    contract = make_contract(str(data_dir), fmt="parquet")
    store = persist_contract(tmp_path, contract)
    data = [
        (1, 101, datetime(2024, 1, 1, 10, 0, 0), 10.0, "EUR"),
    ]
    df = spark.createDataFrame(
        data,
        ["order_id", "customer_id", "order_ts", "amount", "currency"],
    )
    df.write.mode("overwrite").json(str(data_dir))
    with caplog.at_level(logging.WARNING):
        read_with_contract(
            spark,
            contract_id=contract.id,
            contract_store=store,
            expected_contract_version=f"=={contract.version}",
            format="json",
            enforce=False,
        )
    assert any(
        "format json does not match contract server format parquet" in m
        for m in caplog.messages
    )


def test_write_warn_on_format_mismatch(spark, tmp_path: Path, caplog):
    dest_dir = tmp_path / "out"
    contract = make_contract(str(dest_dir), fmt="parquet")
    store = persist_contract(tmp_path, contract)
    data = [
        (1, 101, datetime(2024, 1, 1, 10, 0, 0), 10.0, "EUR"),
    ]
    df = spark.createDataFrame(
        data,
        ["order_id", "customer_id", "order_ts", "amount", "currency"],
    )
    with caplog.at_level(logging.WARNING):
        result = write_with_contract(
            df=df,
            contract_id=contract.id,
            contract_store=store,
            expected_contract_version=f"=={contract.version}",
            path=str(dest_dir),
            format="json",
            mode="overwrite",
            enforce=False,
        )
    assert any(
        "Format json does not match contract server format parquet" in w
        for w in result.warnings
    )
    assert any(
        "format json does not match contract server format parquet" in m.lower()
        for m in caplog.messages
    )


def test_write_split_strategy_creates_auxiliary_datasets(spark, tmp_path: Path):
    base_dir = tmp_path / "split"
    contract = make_contract(str(base_dir))
    store = persist_contract(tmp_path, contract)
    data = [
        (1, 101, datetime(2024, 1, 1, 10, 0, 0), 10.0, "EUR"),
        (2, 102, datetime(2024, 1, 2, 10, 0, 0), 15.5, "INR"),
    ]
    df = spark.createDataFrame(
        data,
        ["order_id", "customer_id", "order_ts", "amount", "currency"],
    )

    strategy = SplitWriteViolationStrategy()
    result = write_with_contract(
        df=df,
        contract_id=contract.id,
        contract_store=store,
        expected_contract_version=f"=={contract.version}",
        mode="overwrite",
        enforce=False,
        violation_strategy=strategy,
    )

    assert result.ok
    assert any("Valid subset written" in warning for warning in result.warnings)
    assert any("Rejected subset written" in warning for warning in result.warnings)
    assert any("outside enum" in warning for warning in result.warnings)

    valid_path = base_dir / strategy.valid_suffix
    reject_path = base_dir / strategy.reject_suffix

    valid_df = spark.read.parquet(str(valid_path))
    reject_df = spark.read.parquet(str(reject_path))

    assert valid_df.count() == 1
    assert reject_df.count() == 1
    assert {row.currency for row in valid_df.collect()} == {"EUR"}
    assert {row.currency for row in reject_df.collect()} == {"INR"}


def test_write_dq_violation_reports_status(spark, tmp_path: Path):
    dest_dir = tmp_path / "dq_out"
    contract = make_contract(str(dest_dir))
    # Tighten quality rule to trigger a violation for the sample data below.
    contract.schema_[0].properties[3].quality = [DataQuality(mustBeGreaterThan=100)]
    store = persist_contract(tmp_path, contract)

    data = [
        (1, 101, datetime(2024, 1, 1, 10, 0, 0), 50.0, "EUR"),
        (2, 102, datetime(2024, 1, 2, 10, 0, 0), 60.0, "USD"),
    ]
    df = spark.createDataFrame(
        data,
        ["order_id", "customer_id", "order_ts", "amount", "currency"],
    )

    dq = StubDQClient(base_path=str(tmp_path / "dq_state"))
    locator = StaticDatasetLocator(dataset_version="dq-out")
    result, status = write_with_contract(
        df=df,
        contract_id=contract.id,
        contract_store=store,
        expected_contract_version=f"=={contract.version}",
        mode="overwrite",
        enforce=False,
        dq_client=dq,
        dataset_locator=locator,
        return_status=True,
    )

    assert result.ok
    assert status is not None
    assert status.status == "block"
    assert status.details and status.details.get("violations", 0) > 0
    with pytest.raises(ValueError):
        write_with_contract(
            df=df,
            contract_id=contract.id,
            contract_store=store,
            expected_contract_version=f"=={contract.version}",
            mode="overwrite",
            enforce=True,
            dq_client=dq,
            dataset_locator=locator,
        )


def test_write_keeps_existing_link_for_contract_upgrade(spark, tmp_path: Path):
    dest_dir = tmp_path / "upgrade"
    contract_v1 = make_contract(str(dest_dir))
    data_ok = [
        (1, 101, datetime(2024, 1, 1, 10, 0, 0), 500.0, "EUR"),
        (2, 102, datetime(2024, 1, 2, 11, 0, 0), 750.0, "USD"),
    ]
    df_ok = spark.createDataFrame(
        data_ok,
        ["order_id", "customer_id", "order_ts", "amount", "currency"],
    )

    dq = StubDQClient(base_path=str(tmp_path / "dq_state_upgrade"))
    store = FSContractStore(str(tmp_path / "upgrade_contracts"))
    store.put(contract_v1)
    upgrade_locator = StaticDatasetLocator(
        dataset_version="2024-01-01",
        dataset_id=f"path:{dest_dir}",
    )
    _, status_ok = write_with_contract(
        df=df_ok,
        contract_id=contract_v1.id,
        contract_store=store,
        expected_contract_version=f"=={contract_v1.version}",
        mode="overwrite",
        enforce=False,
        dq_client=dq,
        dataset_locator=upgrade_locator,
        return_status=True,
    )

    assert status_ok is not None
    assert status_ok.status == "ok"

    dataset_ref = f"path:{dest_dir}"
    assert (
        dq.get_linked_contract_version(dataset_id=dataset_ref)
        == f"{contract_v1.id}:{contract_v1.version}"
    )

    contract_v2 = make_contract(str(dest_dir))
    contract_v2.version = "0.2.0"
    contract_v2.schema_[0].properties[3].quality = [DataQuality(mustBeGreaterThan=800)]
    store.put(contract_v2)

    data_bad = [
        (3, 103, datetime(2024, 1, 3, 12, 0, 0), 200.0, "EUR"),
        (4, 104, datetime(2024, 1, 4, 12, 30, 0), 100.0, "USD"),
    ]
    df_bad = spark.createDataFrame(
        data_bad,
        ["order_id", "customer_id", "order_ts", "amount", "currency"],
    )

    _, status_block = write_with_contract(
        df=df_bad,
        contract_id=contract_v2.id,
        contract_store=store,
        expected_contract_version=f"=={contract_v2.version}",
        mode="overwrite",
        enforce=False,
        dq_client=dq,
        dataset_locator=upgrade_locator,
        return_status=True,
    )

    assert status_block is not None
    assert status_block.status == "block"
    assert status_block.reason and "linked to contract" in status_block.reason
    # Governance keeps the link anchored to the last accepted contract when the
    # submitted version is rejected, so the integration layer should not
    # override it locally.
    assert (
        dq.get_linked_contract_version(dataset_id=dataset_ref)
        == f"{contract_v1.id}:{contract_v1.version}"
    )
