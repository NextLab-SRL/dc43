import os
import json
from pathlib import Path

import pytest

from open_data_contract_standard.model import (
    OpenDataContractStandard,
    SchemaObject,
    SchemaProperty,
    DataQuality,
    Description,
)

from dc43.integration.spark_io import read_with_contract, write_with_contract
from dc43.dq.stub import StubDQClient
from dc43.storage.fs import FSContractStore
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


def test_dq_integration_warn(spark, tmp_path: Path):
    contract = make_contract()
    # Prepare data with one enum violation for currency
    data = [
        (1, 101, datetime(2024, 1, 1, 10, 0, 0), 10.0, "EUR"),
        (2, 102, datetime(2024, 1, 2, 10, 0, 0), 20.5, "INR"),  # violation
    ]
    df = spark.createDataFrame(data, ["order_id", "customer_id", "order_ts", "amount", "currency"])
    data_dir = tmp_path / "parquet"
    df.write.mode("overwrite").format("parquet").save(str(data_dir))

    dq = StubDQClient(base_path=str(tmp_path / "dq_state"), block_on_violation=False)
    # enforce=False to avoid raising on validation expectation failures
    _, status = read_with_contract(
        spark,
        format="parquet",
        path=str(data_dir),
        contract=contract,
        enforce=False,
        dq_client=dq,
        return_status=True,
    )
    assert status is not None
    assert status.status in ("warn", "ok")


def test_write_draft_on_mismatch(spark, tmp_path: Path):
    contract = make_contract()
    # Missing required column 'currency' to trigger validation error
    data = [(1, 101, datetime(2024, 1, 1, 10, 0, 0), 10.0)]
    df = spark.createDataFrame(data, ["order_id", "customer_id", "order_ts", "amount"])
    dest_dir = tmp_path / "out"
    drafts = FSContractStore(str(tmp_path / "drafts"))

    vr, draft = write_with_contract(
        df=df,
        contract=contract,
        path=str(dest_dir),
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
    assert stored.version == draft.version


def test_write_creates_contract_when_missing(spark, tmp_path: Path):
    data = [(1,)]
    df = spark.createDataFrame(data, ["id"])
    dest_dir = tmp_path / "out2"
    store = FSContractStore(str(tmp_path / "contracts"))

    vr = write_with_contract(
        df=df,
        contract_id="demo.output",
        contract_version="1.0.0",
        path=str(dest_dir),
        mode="overwrite",
        draft_store=store,
    )
    assert vr.ok
    stored = store.get("demo.output", "1.0.0")
    assert stored.id == "demo.output"
    assert stored.version == "1.0.0"
