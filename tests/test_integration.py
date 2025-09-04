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

from dc43.integration.spark_io import read_with_contract, write_with_contract
from dc43.dq.stub import StubDQClient
from dc43.storage.fs import FSContractStore
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


def test_dq_integration_warn(spark, tmp_path: Path):
    data_dir = tmp_path / "parquet"
    contract = make_contract(str(data_dir))
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
        contract=contract,
        enforce=False,
        dq_client=dq,
        return_status=True,
    )
    assert status is not None
    assert status.status in ("warn", "ok")


def test_write_draft_on_mismatch(spark, tmp_path: Path):
    dest_dir = tmp_path / "out"
    contract = make_contract(str(dest_dir))
    # Missing required column 'currency' to trigger validation error
    data = [(1, 101, datetime(2024, 1, 1, 10, 0, 0), 10.0)]
    df = spark.createDataFrame(data, ["order_id", "customer_id", "order_ts", "amount"])
    drafts = FSContractStore(str(tmp_path / "drafts"))

    vr, draft = write_with_contract(
        df=df,
        contract=contract,
        mode="overwrite",
        enforce=False,              # continue writing despite mismatch
        draft_on_mismatch=True,
        draft_store=drafts,
        return_draft=True,
    )
    assert draft is not None
    assert draft.status == "draft"
    # persisted
    stored = drafts.get(draft.id, draft.version)
    assert stored.id == draft.id
    assert draft.servers and draft.servers[0].path == str(dest_dir)
    assert draft.servers[0].format == "parquet"


def test_inferred_contract_id_simple(spark, tmp_path: Path):
    dest = tmp_path / "out" / "sample" / "1.0.0"
    df = spark.createDataFrame([(1,)], ["a"])
    drafts = FSContractStore(str(tmp_path / "drafts"))
    vr, draft = write_with_contract(
        df=df,
        path=str(dest),
        format="parquet",
        mode="overwrite",
        draft_on_mismatch=True,
        draft_store=drafts,
        enforce=False,
    )
    assert draft is not None
    assert draft.id == "sample"
    assert drafts.get(draft.id, draft.version).id == "sample"
    assert draft.servers and draft.servers[0].format == "parquet"


def test_write_warn_on_path_mismatch(spark, tmp_path: Path):
    expected_dir = tmp_path / "expected"
    actual_dir = tmp_path / "actual"
    contract = make_contract(str(expected_dir))
    data = [
        (1, 101, datetime(2024, 1, 1, 10, 0, 0), 10.0, "EUR"),
    ]
    df = spark.createDataFrame(
        data,
        ["order_id", "customer_id", "order_ts", "amount", "currency"],
    )
    drafts = FSContractStore(str(tmp_path / "drafts"))
    vr, draft = write_with_contract(
        df=df,
        contract=contract,
        path=str(actual_dir),
        mode="overwrite",
        enforce=False,
        draft_on_mismatch=True,
        draft_store=drafts,
        return_draft=True,
    )
    assert draft is not None
    assert draft.servers and draft.servers[0].path == str(actual_dir)
    assert draft.servers[0].format == "parquet"
    assert any("does not match" in w for w in vr.warnings)


def test_read_warn_on_format_mismatch(spark, tmp_path: Path, caplog):
    data_dir = tmp_path / "json"
    contract = make_contract(str(data_dir), fmt="parquet")
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
            contract=contract,
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
    data = [
        (1, 101, datetime(2024, 1, 1, 10, 0, 0), 10.0, "EUR"),
    ]
    df = spark.createDataFrame(
        data,
        ["order_id", "customer_id", "order_ts", "amount", "currency"],
    )
    drafts = FSContractStore(str(tmp_path / "drafts"))
    with caplog.at_level(logging.WARNING):
        vr, draft = write_with_contract(
            df=df,
            contract=contract,
            path=str(dest_dir),
            format="json",
            mode="overwrite",
            enforce=False,
            draft_on_mismatch=True,
            draft_store=drafts,
            return_draft=True,
        )
    assert any(
        "Format json does not match contract server format parquet" in w
        for w in vr.warnings
    )
    assert any(
        "format json does not match contract server format parquet" in m.lower()
        for m in caplog.messages
    )
    assert draft is not None
    assert draft.servers and draft.servers[0].format == "json"
