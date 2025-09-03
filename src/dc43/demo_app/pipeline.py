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
    contract_id: str,
    contract_version: str,
    input_path: str,
    output_path: str,
    dataset_version: str,
) -> None:
    """Run an example pipeline using the stored contract."""
    spark = SparkSession.builder.appName("dc43-demo").getOrCreate()
    contract = store.get(contract_id, contract_version)
    dq = StubDQClient(base_path=str(Path(DATASETS_FILE).parent / "dq_state"))
    df, status = read_with_contract(
        spark,
        format="json",
        path=input_path,
        contract=contract,
        expected_contract_version=f"=={contract_version}",
        dq_client=dq,
        return_status=True,
    )
    # placeholder transformation could occur here
    write_with_contract(
        df=df,
        contract=contract,
        path=output_path,
        mode="overwrite",
        enforce=True,
    )
    records = load_records()
    records.append(
        DatasetRecord(
            contract_id,
            contract_version,
            dataset_version,
            status.status,
            status.details or {},
        )
    )
    save_records(records)
    spark.stop()

