from __future__ import annotations

import shutil
from pathlib import Path

import pytest
from pyspark.sql import SparkSession

from dc43.demo_app import pipeline


def test_demo_pipeline_records_dq_failure(tmp_path: Path) -> None:
    original_records = pipeline.load_records()
    dq_dir = Path(pipeline.DATASETS_FILE).parent / "dq_state"
    backup = tmp_path / "dq_state_backup"
    if dq_dir.exists():
        shutil.copytree(dq_dir, backup)
    existing_versions = set(pipeline.store.list_versions("orders_enriched"))

    try:
        with pytest.raises(ValueError):
            pipeline.run_pipeline(
                contract_id="orders_enriched",
                contract_version="1.1.0",
                dataset_name=None,
                dataset_version=None,
                run_type="enforce",
                collect_examples=True,
                examples_limit=2,
            )

        updated = pipeline.load_records()
        last = updated[-1]
        out = last.dq_details.get("output", {})
        fails = out.get("failed_expectations", {})

        assert "gt_amount" in fails
        assert fails["gt_amount"]["count"] > 0
        assert out.get("dq_status", {}).get("status") in {"block", "warn", "error"}
        assert last.draft_contract_version == "1.2.0"
        draft_path = (
            Path(pipeline.store.base_path)
            / "orders_enriched"
            / f"{last.draft_contract_version}.json"
        )
        assert draft_path.exists()
    finally:
        pipeline.save_records(original_records)
        if dq_dir.exists():
            shutil.rmtree(dq_dir)
        if backup.exists():
            shutil.copytree(backup, dq_dir)
        new_versions = set(pipeline.store.list_versions("orders_enriched")) - existing_versions
        for ver in new_versions:
            draft_path = Path(pipeline.store.base_path) / "orders_enriched" / f"{ver}.json"
            if draft_path.exists():
                draft_path.unlink()
        # Recreate the shared Spark session stopped by the demo pipeline so
        # subsequent tests relying on the ``spark`` fixture continue to work.
        SparkSession.builder.master("local[2]") \
            .appName("dc43-tests") \
            .config("spark.ui.enabled", "false") \
            .config("spark.sql.shuffle.partitions", "2") \
            .getOrCreate()


def test_demo_pipeline_surfaces_schema_and_dq_failure(tmp_path: Path) -> None:
    original_records = pipeline.load_records()
    dq_dir = Path(pipeline.DATASETS_FILE).parent / "dq_state"
    backup = tmp_path / "dq_state_backup_schema"
    if dq_dir.exists():
        shutil.copytree(dq_dir, backup)
    existing_versions = set(pipeline.store.list_versions("orders_enriched"))

    try:
        with pytest.raises(ValueError) as excinfo:
            pipeline.run_pipeline(
                contract_id="orders_enriched",
                contract_version="2.0.0",
                dataset_name=None,
                dataset_version=None,
                run_type="enforce",
            )

        message = str(excinfo.value)
        assert "DQ violation" in message
        assert "Schema validation failed" in message

        updated = pipeline.load_records()
        last = updated[-1]
        output = last.dq_details.get("output", {})
        assert output.get("errors")
        fails = output.get("failed_expectations", {})
        assert fails
        assert last.violations >= len(output.get("errors", []))
        assert last.draft_contract_version == "2.1.0"
        draft_path = (
            Path(pipeline.store.base_path)
            / "orders_enriched"
            / f"{last.draft_contract_version}.json"
        )
        assert draft_path.exists()
    finally:
        pipeline.save_records(original_records)
        if dq_dir.exists():
            shutil.rmtree(dq_dir)
        if backup.exists():
            shutil.copytree(backup, dq_dir)
        new_versions = set(pipeline.store.list_versions("orders_enriched")) - existing_versions
        for ver in new_versions:
            draft_path = Path(pipeline.store.base_path) / "orders_enriched" / f"{ver}.json"
            if draft_path.exists():
                draft_path.unlink()
        SparkSession.builder.master("local[2]") \
            .appName("dc43-tests") \
            .config("spark.ui.enabled", "false") \
            .config("spark.sql.shuffle.partitions", "2") \
            .getOrCreate()


def test_demo_pipeline_split_strategy_records_auxiliary_datasets(tmp_path: Path) -> None:
    original_records = pipeline.load_records()
    dq_dir = Path(pipeline.DATASETS_FILE).parent / "dq_state"
    backup = tmp_path / "dq_state_backup_split"
    if dq_dir.exists():
        shutil.copytree(dq_dir, backup)
    existing_versions = set(pipeline.store.list_versions("orders_enriched"))

    dataset_version = "split-test"
    final_dataset_name = "orders_enriched"
    final_version = dataset_version
    try:
        final_dataset_name, final_version = pipeline.run_pipeline(
            contract_id="orders_enriched",
            contract_version="1.1.0",
            dataset_name=None,
            dataset_version=dataset_version,
            run_type="observe",
            collect_examples=True,
            examples_limit=2,
            violation_strategy={
                "name": "split",
                "include_valid": True,
                "include_reject": True,
                "write_primary_on_violation": False,
            },
        )

        updated = pipeline.load_records()
        last = updated[-1]
        output = last.dq_details.get("output", {})

        assert output.get("violation_strategy") == "SplitWriteViolationStrategy"
        warnings = output.get("warnings", [])
        assert any("Valid subset written" in w for w in warnings)
        assert any("Rejected subset written" in w for w in warnings)
        assert last.status == "warning"

        aux = output.get("auxiliary_datasets", [])
        assert aux
        aux_map = {entry["kind"]: entry for entry in aux}
        assert {"valid", "reject"}.issubset(aux_map.keys())

        dq_aux = output.get("dq_auxiliary_statuses", [])
        assert dq_aux
        status_map = {entry["dataset_id"]: entry for entry in dq_aux}
        assert "orders_enriched::valid" in status_map
        assert "orders_enriched::reject" in status_map
        assert status_map["orders_enriched::reject"].get("details", {}).get("violations") == 1

        valid_path = Path(aux_map["valid"]["path"])
        reject_path = Path(aux_map["reject"]["path"])
        assert valid_path.exists()
        assert reject_path.exists()

        spark = (
            SparkSession.builder.master("local[2]")
            .appName("dc43-tests")
            .config("spark.ui.enabled", "false")
            .config("spark.sql.shuffle.partitions", "2")
            .getOrCreate()
        )
        assert spark.read.parquet(str(valid_path)).count() > 0
        assert spark.read.parquet(str(reject_path)).count() > 0

        assert last.violations >= 1
        assert last.draft_contract_version == "1.2.0"
    finally:
        pipeline.save_records(original_records)
        if dq_dir.exists():
            shutil.rmtree(dq_dir)
        if backup.exists():
            shutil.copytree(backup, dq_dir)
        new_versions = set(pipeline.store.list_versions("orders_enriched")) - existing_versions
        for ver in new_versions:
            draft_path = Path(pipeline.store.base_path) / "orders_enriched" / f"{ver}.json"
            if draft_path.exists():
                draft_path.unlink()
        try:
            contract = pipeline.store.get("orders_enriched", "1.1.0")
        except FileNotFoundError:
            contract = None
        if contract is not None:
            out_path = pipeline._resolve_output_path(contract, final_dataset_name, final_version)
            if out_path.exists():
                shutil.rmtree(out_path)
        SparkSession.builder.master("local[2]") \
            .appName("dc43-tests") \
            .config("spark.ui.enabled", "false") \
            .config("spark.sql.shuffle.partitions", "2") \
            .getOrCreate()


def test_demo_pipeline_strict_split_marks_error(tmp_path: Path) -> None:
    original_records = pipeline.load_records()
    dq_dir = Path(pipeline.DATASETS_FILE).parent / "dq_state"
    backup = tmp_path / "dq_state_backup_split_strict"
    if dq_dir.exists():
        shutil.copytree(dq_dir, backup)
    existing_versions = set(pipeline.store.list_versions("orders_enriched"))

    dataset_version = "split-strict-test"
    try:
        pipeline.run_pipeline(
            contract_id="orders_enriched",
            contract_version="1.1.0",
            dataset_name=None,
            dataset_version=dataset_version,
            run_type="observe",
            collect_examples=True,
            examples_limit=2,
            violation_strategy={
                "name": "split-strict",
                "include_valid": True,
                "include_reject": True,
                "write_primary_on_violation": False,
                "failure_message": "Reject rows are not permitted",
            },
        )

        updated = pipeline.load_records()
        last = updated[-1]
        output = last.dq_details.get("output", {})

        assert last.status == "error"
        assert "Reject rows are not permitted" in output.get("errors", [])
        warnings = output.get("warnings", [])
        assert any("Rejected subset written" in w for w in warnings)
        dq_status = output.get("dq_status", {})
        assert dq_status.get("status") in {"ok", "warn", "warning"}
        assert last.draft_contract_version == "1.2.0"
    finally:
        pipeline.save_records(original_records)
        if dq_dir.exists():
            shutil.rmtree(dq_dir)
        if backup.exists():
            shutil.copytree(backup, dq_dir)
        new_versions = set(pipeline.store.list_versions("orders_enriched")) - existing_versions
        for ver in new_versions:
            draft_path = Path(pipeline.store.base_path) / "orders_enriched" / f"{ver}.json"
            if draft_path.exists():
                draft_path.unlink()
        try:
            contract = pipeline.store.get("orders_enriched", "1.1.0")
        except FileNotFoundError:
            contract = None
        if contract is not None:
            out_path = pipeline._resolve_output_path(contract, "orders_enriched", dataset_version)
            if out_path.exists():
                shutil.rmtree(out_path)
        SparkSession.builder.master("local[2]") \
            .appName("dc43-tests") \
            .config("spark.ui.enabled", "false") \
            .config("spark.sql.shuffle.partitions", "2") \
            .getOrCreate()
