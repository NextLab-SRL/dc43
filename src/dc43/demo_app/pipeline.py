from __future__ import annotations

"""Example transformation pipeline using dc43 helpers.

This script demonstrates how a Spark job might read data with contract
validation, perform transformations (omitted) and write the result while
recording the dataset version in the demo app's registry.
"""

from pathlib import Path

from dc43.demo_app.server import (
    store,
    DATASETS_FILE,
    DatasetRecord,
    load_records,
    save_records,
    set_contract_status,
)
from dc43.dq.stub import StubDQClient
from dc43.dq.metrics import compute_metrics
from dc43.integration.spark_io import write_with_contract
from pyspark.sql import SparkSession


def run_pipeline(
    contract_id: str,
    contract_version: str,
    input_path: str,
    output_path: str,
    dataset_version: str,
    *,
    mode: str = "contract",
) -> None:
    """Run an example pipeline using the stored contract.

    Parameters
    ----------
    mode:
        ``"contract"``      - enforce contract and run DQ metrics.
        ``"draft"``         - allow mismatches and create a draft contract.
        ``"invalid"``       - intentionally produce data violating expectations.
    """

    spark = SparkSession.builder.appName("dc43-demo").getOrCreate()
    contract = store.get(contract_id, contract_version)
    dq = StubDQClient(base_path=str(Path(DATASETS_FILE).parent / "dq_state"))

    df = spark.read.json(input_path)

    status_str = "unknown"
    details = {}

    if mode == "draft":
        vr, draft = write_with_contract(
            df=df,
            contract=contract,
            path=output_path,
            mode="overwrite",
            enforce=False,
            draft_on_mismatch=True,
            draft_store=store,
            return_draft=True,
        )
        if draft is not None:
            contract_version = draft.version
            set_contract_status(contract_id, contract_version, "draft")
    else:
        if mode == "invalid":
            df = df.selectExpr("order_id as id", "-abs(amount) as amount")
        else:  # "contract" flow
            df = df.selectExpr("order_id as id", "amount")

        write_with_contract(
            df=df,
            contract=contract,
            path=output_path,
            mode="overwrite",
            enforce=True,
        )

        metrics = compute_metrics(df, contract)
        dq_status = dq.submit_metrics(
            contract=contract,
            dataset_id=output_path,
            dataset_version=dataset_version,
            metrics=metrics,
        )
        status_str = dq_status.status
        details = dq_status.details or {}

    records = load_records()
    records.append(
        DatasetRecord(
            contract_id,
            contract_version,
            dataset_version,
            status_str,
            details,
        )
    )
    save_records(records)
    spark.stop()

