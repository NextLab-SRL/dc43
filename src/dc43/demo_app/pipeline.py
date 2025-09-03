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
)
from dc43.dq.stub import StubDQClient
from dc43.integration.spark_io import read_with_contract, write_with_contract
from pyspark.sql import SparkSession


def run_pipeline(
    input_contract_id: str,
    input_contract_version: str,
    output_contract_id: str,
    output_contract_version: str,
    input_path: str,
    output_path: str,
    dataset_version: str,
) -> None:
    """Run an example pipeline using separate input/output contracts."""

    spark = SparkSession.builder.appName("dc43-demo").getOrCreate()

    in_contract = store.get(input_contract_id, input_contract_version)
    dq = StubDQClient(base_path=str(Path(DATASETS_FILE).parent / "dq_state"))
    df, in_status = read_with_contract(
        spark,
        format="json",
        path=input_path,
        contract=in_contract,
        expected_contract_version=f"=={input_contract_version}",
        dq_client=dq,
        return_status=True,
    )
    if in_status and in_status.status == "block":
        spark.stop()
        raise ValueError(f"Input dataset blocked: {in_status.reason or in_status.details}")

    # placeholder transformation could occur here

    _, out_status = write_with_contract(
        df=df,
        contract_id=output_contract_id,
        contract_version=output_contract_version,
        path=output_path,
        mode="overwrite",
        enforce=True,
        draft_on_mismatch=True,
        draft_store=store,
        dq_client=dq,
        dataset_version=dataset_version,
        return_status=True,
    )

    records = load_records()
    records.append(
        DatasetRecord(
            output_contract_id,
            output_contract_version,
            dataset_version,
            out_status.status if out_status else "unknown",
            (out_status.details if out_status else {}),
        )
    )
    save_records(records)
    spark.stop()

