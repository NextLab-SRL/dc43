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
from dc43.integration.dataset import dataset_id_from_ref
from pyspark.sql import SparkSession


def _next_version(existing: list[str]) -> str:
    """Return the next patch version given existing semver strings."""
    if not existing:
        return "1.0.0"
    parts = [list(map(int, v.split("."))) for v in existing]
    major, minor, patch = max(parts)
    return f"{major}.{minor}.{patch + 1}"


def run_pipeline(
    contract_id: str,
    contract_version: str,
    dataset_name: str,
    dataset_version: str | None,
    run_type: str,
    input_path: str,
) -> str:
    """Run an example pipeline using the stored contract."""
    spark = SparkSession.builder.appName("dc43-demo").getOrCreate()
    contract = store.get(contract_id, contract_version)
    dq = StubDQClient(base_path=str(Path(DATASETS_FILE).parent / "dq_state"))
    ds_id = dataset_id_from_ref(path=input_path)
    dq.link_dataset_contract(
        dataset_id=ds_id,
        dataset_version="1.0.0",
        contract_id=contract_id,
        contract_version=contract_version,
    )
    df, status = read_with_contract(
        spark,
        format="json",
        path=input_path,
        contract=contract,
        expected_contract_version=f"=={contract_version}",
        dq_client=dq,
        dataset_id=ds_id,
        dataset_version="1.0.0",
        return_status=True,
    )
    # placeholder transformation could occur here
    records = load_records()
    if not dataset_version:
        existing = [r.dataset_version for r in records if r.dataset_name == dataset_name]
        dataset_version = _next_version(existing)
    server = (contract.servers or [None])[0]
    base_path = Path(getattr(server, "path", "")) if server else Path()
    if not base_path.is_absolute():
        base_path = Path(DATASETS_FILE).parent / base_path
    output_path = base_path / dataset_name / dataset_version
    output_path.parent.mkdir(parents=True, exist_ok=True)
    write_with_contract(
        df=df,
        contract=contract,
        path=str(output_path),
        mode="overwrite",
        enforce=True,
    )
    records.append(
        DatasetRecord(
            contract_id,
            contract_version,
            dataset_name,
            dataset_version,
            status.status,
            status.details or {},
            run_type,
        )
    )
    save_records(records)
    spark.stop()
    return dataset_version
