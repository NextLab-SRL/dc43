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
    DATA_INPUT_DIR,
    DatasetRecord,
    load_records,
    save_records,
    load_contract_meta,
    save_contract_meta,
)
from dc43.dq.stub import StubDQClient
from dc43.dq.metrics import compute_metrics, expectations_from_contract
from dc43.integration.spark_io import read_with_contract, write_with_contract
from dc43.integration.validation import apply_contract
from pyspark.sql import SparkSession


def _next_version(existing: list[str]) -> str:
    """Return the next patch version given existing semver strings."""
    if not existing:
        return "1.0.0"
    parts = [list(map(int, v.split("."))) for v in existing]
    major, minor, patch = max(parts)
    return f"{major}.{minor}.{patch + 1}"


def run_pipeline(
    contract_id: str | None,
    contract_version: str | None,
    dataset_name: str,
    dataset_version: str | None,
    run_type: str,
) -> str:
    """Run an example pipeline using the stored contract."""
    spark = SparkSession.builder.appName("dc43-demo").getOrCreate()
    dq = StubDQClient(base_path=str(Path(DATASETS_FILE).parent / "dq_state"))

    # Read primary orders dataset with its contract
    orders_contract = store.get("orders", "1.1.0")
    orders_path = str(DATA_INPUT_DIR / "orders.json")
    dq.link_dataset_contract(
        dataset_id="orders",
        dataset_version="1.0.0",
        contract_id="orders",
        contract_version="1.1.0",
    )
    orders_df, orders_status = read_with_contract(
        spark,
        format="json",
        path=orders_path,
        contract=orders_contract,
        expected_contract_version="==1.1.0",
        dq_client=dq,
        dataset_id="orders",
        dataset_version="1.0.0",
    )

    # Join with customers lookup dataset
    customers_contract = store.get("customers", "1.0.0")
    customers_path = str(DATA_INPUT_DIR / "customers.json")
    dq.link_dataset_contract(
        dataset_id="customers",
        dataset_version="1.0.0",
        contract_id="customers",
        contract_version="1.0.0",
    )
    customers_df, customers_status = read_with_contract(
        spark,
        format="json",
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
    server = (output_contract.servers or [None])[0] if output_contract else None
    data_root = Path(DATA_INPUT_DIR).parent
    base_path = Path(getattr(server, "path", "")) if server else data_root
    if not base_path.is_absolute():
        base_path = data_root / base_path
    output_path = base_path / dataset_name / dataset_version
    output_path.parent.mkdir(parents=True, exist_ok=True)
    error: Exception | None = None
    output_details = {}
    output_status = None
    try:
        result, draft = write_with_contract(
            df=df,
            contract=output_contract,
            path=str(output_path),
            mode="overwrite",
            enforce=False,
            draft_on_mismatch=True,
            draft_store=store,
        )
        if output_contract:
            dq.link_dataset_contract(
                dataset_id=dataset_name,
                dataset_version=dataset_version,
                contract_id=contract_id or output_contract.id,
                contract_version=contract_version or output_contract.version,
            )
            aligned_df = apply_contract(df, output_contract)
            metrics = compute_metrics(aligned_df, output_contract)
            output_status = dq.submit_metrics(
                contract=output_contract,
                dataset_id=dataset_name,
                dataset_version=dataset_version,
                metrics=metrics,
            )
            output_details = {**result.details, **output_status.details}
            if output_status.status != "ok":
                failed_examples = {}
                exps = expectations_from_contract(output_contract)
                for key, expr in exps.items():
                    metric_key = f"violations.{key}"
                    if output_status.details["metrics"].get(metric_key, 0) > 0:
                        failed_examples[key] = [
                            r.asDict()
                            for r in aligned_df.filter(f"NOT ({expr})").limit(5).collect()
                        ]
                if failed_examples:
                    output_details["failed_examples"] = failed_examples
                if run_type == "enforce":
                    error = ValueError(f"DQ violation: {output_status.details}")
        else:
            output_details = result.details
            if run_type == "enforce":
                error = ValueError("Contract required for existing mode")
        if result.ok is False and error is None:
            error = ValueError(f"Contract validation failed: {result.errors}")
    except ValueError as exc:
        error = exc
        if output_contract:
            from dc43.versioning import SemVer

            next_ver = str(SemVer.parse(contract_version).bump("minor")) if contract_version else "0.0.1"
            meta = load_contract_meta()
            meta.append({"id": contract_id, "version": next_ver, "status": "draft"})
            save_contract_meta(meta)
    else:
        if draft:
            meta = load_contract_meta()
            meta.append({"id": draft.id, "version": draft.version, "status": "draft"})
            save_contract_meta(meta)
            contract_id = draft.id
            contract_version = draft.version

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
        )
    )
    save_records(records)
    spark.stop()
    if error:
        raise error
    return dataset_version
