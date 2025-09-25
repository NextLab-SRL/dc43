from __future__ import annotations

"""Example transformation pipeline using dc43 helpers.

This script demonstrates how a Spark job might read data with contract
validation, perform transformations (omitted) and write the result while
recording the dataset version in the demo app's registry.
"""

from pathlib import Path
from typing import Any

from dc43.demo_app.server import (
    store,
    DATASETS_FILE,
    DATA_DIR,
    DatasetRecord,
    load_records,
    save_records,
)
from dc43.components.data_quality import DataQualityManager
from dc43.components.data_quality.integration import attach_failed_expectations
from dc43.components.data_quality.governance.stubs import StubDQClient
from dc43.components.integration.spark_io import read_with_contract, write_with_contract
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
    dataset_name: str | None,
    dataset_version: str | None,
    run_type: str,
    collect_examples: bool = False,
    examples_limit: int = 5,
    ) -> tuple[str, str]:
    """Run an example pipeline using the stored contract.

    When an output contract is supplied the dataset name is derived from the
    contract identifier so the recorded runs and filesystem layout match the
    declared server path.  Callers may supply a custom name when no contract is
    available.  Returns the dataset name used along with the materialized
    version.
    """
    existing_session = SparkSession.getActiveSession()
    spark = SparkSession.builder.appName("dc43-demo").getOrCreate()
    dq_client = StubDQClient(base_path=str(Path(DATASETS_FILE).parent / "dq_state"))
    dq = DataQualityManager(dq_client, draft_store=store)

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
    output_contract = (
        store.get(contract_id, contract_version) if contract_id and contract_version else None
    )
    if output_contract and getattr(output_contract, "id", None):
        # Align dataset naming with the contract so recorded versions and paths
        # remain consistent with the declared server definition.
        dataset_name = output_contract.id
    elif not dataset_name:
        dataset_name = contract_id or "result"
    if not dataset_version:
        existing = [r.dataset_version for r in records if r.dataset_name == dataset_name]
        dataset_version = _next_version(existing)

    assert dataset_name
    assert dataset_version
    output_path = _resolve_output_path(output_contract, dataset_name, dataset_version)
    server = (output_contract.servers or [None])[0] if output_contract else None

    result, output_status = write_with_contract(
        df=df,
        contract=output_contract,
        path=str(output_path),
        format=getattr(server, "format", "parquet"),
        mode="overwrite",
        enforce=False,
        dq_client=dq,
        dataset_id=dataset_name,
        dataset_version=dataset_version,
        return_status=True,
    )

    if output_status and output_contract:
        output_status = attach_failed_expectations(output_contract, output_status)

    error: ValueError | None = None
    if run_type == "enforce":
        if not output_contract:
            error = ValueError("Contract required for existing mode")
        else:
            issues: list[str] = []
            if output_status and output_status.status != "ok":
                detail_msg: dict[str, Any] = dict(output_status.details or {})
                if output_status.reason:
                    detail_msg["reason"] = output_status.reason
                issues.append(
                    f"DQ violation: {detail_msg or output_status.status}"
                )
            if not result.ok:
                issues.append(
                    f"Schema validation failed: {result.errors}"
                )
            if issues:
                error = ValueError("; ".join(issues))

    draft_version: str | None = None
    output_details = result.details.copy()
    dq_payload: dict[str, Any] = {}
    if output_status:
        dq_payload = dict(output_status.details or {})
        dq_payload.setdefault("status", output_status.status)
        if output_status.reason:
            dq_payload.setdefault("reason", output_status.reason)

        dq_metrics = dq_payload.get("metrics", {})
        if dq_metrics:
            merged_metrics = {**dq_metrics, **output_details.get("metrics", {})}
            output_details["metrics"] = merged_metrics
        if "violations" in dq_payload:
            output_details["violations"] = dq_payload["violations"]
        if "failed_expectations" in dq_payload:
            output_details["failed_expectations"] = dq_payload["failed_expectations"]

        summary = dict(output_details.get("dq_status", {}))
        summary.setdefault("status", dq_payload.get("status", output_status.status))
        if dq_payload.get("reason"):
            summary.setdefault("reason", dq_payload["reason"])
        extras = {
            k: v
            for k, v in dq_payload.items()
            if k
            not in ("metrics", "violations", "failed_expectations", "status", "reason")
        }
        if extras:
            summary.update(extras)
        if summary:
            output_details["dq_status"] = summary

    draft_version = output_details.get("draft_contract_version")
    if not draft_version and dq_payload:
        draft_version = dq_payload.get("draft_contract_version")
    if draft_version:
        output_details.setdefault("draft_contract_version", draft_version)

    combined_details = {
        "orders": orders_status.details if orders_status else None,
        "customers": customers_status.details if customers_status else None,
        "output": output_details,
    }
    total_violations = 0
    for det in combined_details.values():
        if not det or not isinstance(det, dict):
            continue
        total_violations += int(det.get("violations", 0) or 0)
        errs = det.get("errors")
        if isinstance(errs, list):
            total_violations += len(errs)
        fails = det.get("failed_expectations")
        if isinstance(fails, dict):
            total_violations += sum(int(info.get("count", 0) or 0) for info in fails.values())

    status_value = "ok"
    if (
        (orders_status and orders_status.status != "ok")
        or (customers_status and customers_status.status != "ok")
        or (output_status and output_status.status != "ok")
        or result.errors
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
    if not existing_session:
        spark.stop()
    if error:
        raise error
    return dataset_name, dataset_version
