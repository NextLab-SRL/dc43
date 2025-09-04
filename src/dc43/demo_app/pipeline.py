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
    DATA_DIR,
    DatasetRecord,
    load_records,
    save_records,
)
from dc43.dq.stub import StubDQClient
from dc43.dq.spark_metrics import attach_failed_expectations
from dc43.integration.spark_io import read_with_contract, write_with_contract
from open_data_contract_standard.model import OpenDataContractStandard
from pyspark.sql import SparkSession


def _next_version(existing: list[str]) -> str:
    """Return the next patch version given existing semver strings."""
    if not existing:
        return "1.0.0"
    parts = [list(map(int, v.split("."))) for v in existing]
    major, minor, patch = max(parts)
    return f"{major}.{minor}.{patch + 1}"


def _resolve_output_path(
    contract: OpenDataContractStandard | None,
    dataset_name: str,
    dataset_version: str,
) -> Path:
    """Return output path for dataset relative to contract servers."""
    server = (contract.servers or [None])[0] if contract else None
    data_root = Path(DATA_DIR).parent
    base_path = Path(getattr(server, "path", "")) if server else data_root
    if base_path.suffix:
        base_path = base_path.parent
    if not base_path.is_absolute():
        base_path = data_root / base_path
    out = base_path / dataset_name / dataset_version
    out.parent.mkdir(parents=True, exist_ok=True)
    return out


def run_pipeline(
    contract_id: str | None,
    contract_version: str | None,
    dataset_name: str,
    dataset_version: str | None,
    run_type: str,
    collect_examples: bool = False,
    examples_limit: int = 5,
) -> str:
    """Run an example pipeline using the stored contract."""
    spark = SparkSession.builder.appName("dc43-demo").getOrCreate()
    dq = StubDQClient(base_path=str(Path(DATASETS_FILE).parent / "dq_state"))

    # Read primary orders dataset with its contract
    orders_contract = store.get("orders", "1.1.0")
    orders_path = str(DATA_DIR / "orders/1.1.0/orders.json")
    orders_df, orders_status = read_with_contract(
        spark,
        path=orders_path,
        contract=orders_contract,
        expected_contract_version="==1.1.0",
        dq_client=dq,
        dataset_id="orders",
        dataset_version="1.1.0",
    )

    # Join with customers lookup dataset
    customers_contract = store.get("customers", "1.0.0")
    customers_path = str(DATA_DIR / "customers/1.0.0/customers.json")
    customers_df, customers_status = read_with_contract(
        spark,
        path=customers_path,
        contract=customers_contract,
        expected_contract_version="==1.0.0",
        dq_client=dq,
        dataset_id="customers",
        dataset_version="1.0.0",
    )

    df = orders_df.join(customers_df, "customer_id")

    records = load_records()
    if not dataset_version:
        existing = [r.dataset_version for r in records if r.dataset_name == dataset_name]
        dataset_version = _next_version(existing)

    output_contract = (
        store.get(contract_id, contract_version) if contract_id and contract_version else None
    )
    output_path = _resolve_output_path(output_contract, dataset_name, dataset_version)
    server = (output_contract.servers or [None])[0] if output_contract else None

    result, output_status, draft = write_with_contract(
        df=df,
        contract=output_contract,
        path=str(output_path),
        format=getattr(server, "format", "parquet"),
        mode="overwrite",
        enforce=False,
        draft_on_mismatch=True,
        draft_store=store,
        dq_client=dq,
        dataset_id=dataset_name,
        dataset_version=dataset_version,
        return_status=True,
    )

    if output_status and output_contract:
        output_status = attach_failed_expectations(
            df,
            output_contract,
            output_status,
            collect_examples=collect_examples,
            examples_limit=examples_limit,
        )

    error: ValueError | None = None
    if run_type == "enforce":
        if not output_contract:
            error = ValueError("Contract required for existing mode")
        elif output_status and output_status.status != "ok":
            error = ValueError(f"DQ violation: {output_status.details}")
        elif not result.ok:
            error = ValueError(f"Contract validation failed: {result.errors}")

    draft_version: str | None = draft.version if draft else None
    output_details = {**result.details, **(output_status.details if output_status else {})}

    combined_details = {
        "orders": orders_status.details if orders_status else None,
        "customers": customers_status.details if customers_status else None,
        "output": output_details,
    }
    total_violations = 0
    for det in combined_details.values():
        if det and isinstance(det, dict):
            total_violations += int(det.get("violations", 0))
    status_value = "ok"
    if (
        (orders_status and orders_status.status != "ok")
        or (customers_status and customers_status.status != "ok")
        or (output_status and output_status.status != "ok")
        or error is not None
    ):
        status_value = "error"
    records.append(
        DatasetRecord(
            contract_id or "",
            contract_version or "",
            dataset_name,
            dataset_version,
            status_value,
            combined_details,
            run_type,
            total_violations,
            draft_contract_version=draft_version,
        )
    )
    save_records(records)
    spark.stop()
    if error:
        raise error
    return dataset_version
