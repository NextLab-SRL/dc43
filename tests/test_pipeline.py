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
        assert last.draft_contract_version is None
    finally:
        pipeline.save_records(original_records)
        if dq_dir.exists():
            shutil.rmtree(dq_dir)
        if backup.exists():
            shutil.copytree(backup, dq_dir)
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
    finally:
        pipeline.save_records(original_records)
        if dq_dir.exists():
            shutil.rmtree(dq_dir)
        if backup.exists():
            shutil.copytree(backup, dq_dir)
        SparkSession.builder.master("local[2]") \
            .appName("dc43-tests") \
            .config("spark.ui.enabled", "false") \
            .config("spark.sql.shuffle.partitions", "2") \
            .getOrCreate()
