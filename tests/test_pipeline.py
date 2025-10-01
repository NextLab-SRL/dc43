from __future__ import annotations

import json
import shutil
from pathlib import Path

import pytest
from pyspark.sql import SparkSession

from dc43_demo_app import pipeline
from dc43_demo_app.contracts_workspace import prepare_demo_workspace

prepare_demo_workspace()


def test_demo_pipeline_records_dq_failure(tmp_path: Path) -> None:
    original_records = pipeline.load_records()
    dq_dir = Path(pipeline.DATASETS_FILE).parent / "dq_state"
    backup = tmp_path / "dq_state_backup"
    if dq_dir.exists():
        shutil.copytree(dq_dir, backup)
    existing_versions = set(pipeline.store.list_versions("orders_enriched"))

    try:
        with pytest.raises(ValueError) as excinfo:
            pipeline.run_pipeline(
                contract_id="orders_enriched",
                contract_version="1.1.0",
                dataset_name=None,
                dataset_version=None,
                run_type="enforce",
                collect_examples=True,
                examples_limit=2,
            )

        message = str(excinfo.value)
        assert "DQ violation" in message
        assert "Schema validation failed" not in message

        updated = pipeline.load_records()
        last = updated[-1]
        out = last.dq_details.get("output", {})
        fails = out.get("failed_expectations", {})

        assert "gt_amount" in fails
        assert fails["gt_amount"]["count"] > 0
        assert out.get("dq_status", {}).get("status") in {"block", "warn", "error"}
        assert last.draft_contract_version
        assert last.draft_contract_version.startswith("1.2.0-draft-")
        draft_path = (
            Path(pipeline.store.base_path)
            / "orders_enriched"
            / f"{last.draft_contract_version}.json"
        )
        assert draft_path.exists()
        payload = json.loads(draft_path.read_text())
        properties = {
            entry.get("property"): entry.get("value")
            for entry in payload.get("customProperties", [])
        }
        context = properties.get("draft_context") or {}
        assert context.get("pipeline") == "dc43_demo_app.pipeline.run_pipeline"
        assert context.get("module") == "dc43_demo_app.pipeline"
        assert context.get("function") == "run_pipeline"
        assert context.get("dataset_id") == "orders_enriched"
        assert context.get("dataset_version") == last.dataset_version
        assert context.get("step") == "output-write"
        assert context.get("run_type") == "enforce"
        assert "run_id" in context

        activity = out.get("pipeline_activity") or []
        assert activity
        write_events = [
            event
            for entry in activity
            for event in entry.get("events", [])
            if isinstance(event, dict) and event.get("operation") == "write"
        ]
        assert write_events
        for event in write_events:
            ctx = event.get("pipeline_context") or {}
            assert ctx.get("step") == "output-write"
            assert ctx.get("run_type") == "enforce"
            assert ctx.get("dataset_version") == last.dataset_version
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
        assert last.draft_contract_version
        assert last.draft_contract_version.startswith("2.1.0-draft-")
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
                "write_primary_on_violation": True,
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
        assert "orders_enriched" in status_map
        assert "orders_enriched::valid" in status_map
        assert "orders_enriched::reject" in status_map
        primary_entry = status_map["orders_enriched"]
        assert primary_entry.get("status") == "warn"
        primary_details = (
            primary_entry.get("details") if isinstance(primary_entry.get("details"), dict) else {}
        )
        assert primary_details.get("status_before_override") == "block"
        overrides = primary_details.get("overrides", []) or []
        assert any(
            "Primary DQ status downgraded" in str(note)
            for note in overrides
        )
        assert (
            status_map["orders_enriched::reject"].get("details", {}).get("violations")
            >= 1
        )

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
        assert last.draft_contract_version
        assert last.draft_contract_version.startswith("1.2.0-draft-")
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
        assert last.draft_contract_version
        assert last.draft_contract_version.startswith("1.2.0-draft-")
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


def test_demo_pipeline_invalid_read_block(tmp_path: Path) -> None:
    original_records = pipeline.load_records()
    dq_dir = Path(pipeline.DATASETS_FILE).parent / "dq_state"
    backup = tmp_path / "dq_state_backup_invalid_block"
    if dq_dir.exists():
        shutil.copytree(dq_dir, backup)

    pipeline.set_active_version("orders", "2025-09-28")
    pipeline.set_active_version("orders__valid", "2025-09-28")
    pipeline.set_active_version("orders__reject", "2025-09-28")

    try:
        with pytest.raises(ValueError) as excinfo:
            pipeline.run_pipeline(
                contract_id="orders_enriched",
                contract_version="1.1.0",
                dataset_name=None,
                dataset_version=None,
                run_type="enforce",
                inputs={
                    "orders": {
                        "dataset_version": "latest",
                    }
                },
            )

        assert "DQ status is blocking" in str(excinfo.value)
        updated_records = pipeline.load_records()
        assert updated_records != original_records
        last = updated_records[-1]
        assert last.dataset_name == "orders_enriched"
        assert last.status == "error"
        assert last.reason.startswith("DQ status is blocking")
        orders_details = last.dq_details.get("orders", {})
        assert orders_details.get("status") == "block"
        assert "duplicate" in json.dumps(orders_details).lower()
        output_details = last.dq_details.get("output", {})
        assert output_details.get("dq_status", {}).get("reason", "").startswith(
            "DQ status is blocking"
        )
    finally:
        pipeline.save_records(original_records)
        if dq_dir.exists():
            shutil.rmtree(dq_dir)
        if backup.exists():
            shutil.copytree(backup, dq_dir)
        pipeline.set_active_version("orders", "2024-01-01")
        pipeline.set_active_version("orders__valid", "2025-09-28")
        pipeline.set_active_version("orders__reject", "2025-09-28")
        SparkSession.builder.master("local[2]") \
            .appName("dc43-tests") \
            .config("spark.ui.enabled", "false") \
            .config("spark.sql.shuffle.partitions", "2") \
            .getOrCreate()


def test_demo_pipeline_valid_subset_read(tmp_path: Path) -> None:
    original_records = pipeline.load_records()
    dq_dir = Path(pipeline.DATASETS_FILE).parent / "dq_state"
    backup = tmp_path / "dq_state_backup_valid_subset"
    if dq_dir.exists():
        shutil.copytree(dq_dir, backup)
    existing_versions = set(pipeline.store.list_versions("orders_enriched"))
    dataset_name = "orders_enriched"
    dataset_version = "valid-ok"

    pipeline.set_active_version("orders", "2025-09-28")
    pipeline.set_active_version("orders__valid", "2025-09-28")
    pipeline.set_active_version("orders__reject", "2025-09-28")

    try:
        dataset_name, dataset_version = pipeline.run_pipeline(
            contract_id="orders_enriched",
            contract_version="1.1.0",
            dataset_name=None,
            dataset_version="valid-ok",
            run_type="observe",
            collect_examples=True,
            examples_limit=2,
            inputs={
                "orders": {
                    "dataset_id": "orders::valid",
                    "dataset_version": "latest__valid",
                }
            },
        )

        assert dataset_name == "orders_enriched"
        updated = pipeline.load_records()
        last = updated[-1]
        assert last.dataset_version == dataset_version
        assert last.status == "ok"
        orders_details = last.dq_details.get("orders", {})
        metrics = orders_details.get("metrics", {})
        assert metrics.get("row_count", 0) >= 1
    finally:
        pipeline.save_records(original_records)
        if dq_dir.exists():
            shutil.rmtree(dq_dir)
        if backup.exists():
            shutil.copytree(backup, dq_dir)
        pipeline.set_active_version("orders", "2024-01-01")
        pipeline.set_active_version("orders__valid", "2025-09-28")
        pipeline.set_active_version("orders__reject", "2025-09-28")
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
            out_path = pipeline._resolve_output_path(contract, dataset_name, dataset_version)
            if out_path.exists():
                shutil.rmtree(out_path)
        SparkSession.builder.master("local[2]") \
            .appName("dc43-tests") \
            .config("spark.ui.enabled", "false") \
            .config("spark.sql.shuffle.partitions", "2") \
            .getOrCreate()


def test_demo_pipeline_valid_subset_invalid_output(tmp_path: Path) -> None:
    original_records = pipeline.load_records()
    dq_dir = Path(pipeline.DATASETS_FILE).parent / "dq_state"
    backup = tmp_path / "dq_state_backup_valid_subset_violation"
    if dq_dir.exists():
        shutil.copytree(dq_dir, backup)
    existing_versions = set(pipeline.store.list_versions("orders_enriched"))
    forced_version = "valid-invalid"
    created_dataset_version: str | None = None

    pipeline.set_active_version("orders", "2025-09-28")
    pipeline.set_active_version("orders__valid", "2025-09-28")
    pipeline.set_active_version("orders__reject", "2025-09-28")

    try:
        with pytest.raises(ValueError):
            pipeline.run_pipeline(
                contract_id="orders_enriched",
                contract_version="1.1.0",
                dataset_name=None,
                dataset_version=forced_version,
                run_type="enforce",
                collect_examples=True,
                examples_limit=2,
                inputs={
                    "orders": {
                        "dataset_id": "orders::valid",
                        "dataset_version": "latest__valid",
                    }
                },
                output_adjustment="valid-subset-violation",
            )

        updated = pipeline.load_records()
        last = updated[-1]
        created_dataset_version = last.dataset_version
        assert created_dataset_version == forced_version
        assert last.status == "error"
        output_details = last.dq_details.get("output", {})
        dq_summary = output_details.get("dq_status", {}) or {}
        assert dq_summary.get("status") in {"block", "error", "warn"}
        transformations = output_details.get("transformations", [])
        assert any("downgraded" in str(note) for note in transformations)
    finally:
        pipeline.save_records(original_records)
        if dq_dir.exists():
            shutil.rmtree(dq_dir)
        if backup.exists():
            shutil.copytree(backup, dq_dir)
        pipeline.set_active_version("orders", "2024-01-01")
        pipeline.set_active_version("orders__valid", "2025-09-28")
        pipeline.set_active_version("orders__reject", "2025-09-28")
        new_versions = set(pipeline.store.list_versions("orders_enriched")) - existing_versions
        for ver in new_versions:
            draft_path = Path(pipeline.store.base_path) / "orders_enriched" / f"{ver}.json"
            if draft_path.exists():
                draft_path.unlink()
        if created_dataset_version:
            try:
                contract = pipeline.store.get("orders_enriched", "1.1.0")
            except FileNotFoundError:
                contract = None
            if contract is not None:
                out_path = pipeline._resolve_output_path(
                    contract,
                    "orders_enriched",
                    created_dataset_version,
                )
                if out_path.exists():
                    shutil.rmtree(out_path)
        SparkSession.builder.master("local[2]") \
            .appName("dc43-tests") \
            .config("spark.ui.enabled", "false") \
            .config("spark.sql.shuffle.partitions", "2") \
            .getOrCreate()


def test_demo_pipeline_full_override_read(tmp_path: Path) -> None:
    original_records = pipeline.load_records()
    dq_dir = Path(pipeline.DATASETS_FILE).parent / "dq_state"
    backup = tmp_path / "dq_state_backup_full_override"
    if dq_dir.exists():
        shutil.copytree(dq_dir, backup)
    existing_versions = set(pipeline.store.list_versions("orders_enriched"))
    dataset_name = "orders_enriched"
    dataset_version = "override-full"

    pipeline.set_active_version("orders", "2025-09-28")
    pipeline.set_active_version("orders__valid", "2025-09-28")
    pipeline.set_active_version("orders__reject", "2025-09-28")

    try:
        dataset_name, dataset_version = pipeline.run_pipeline(
            contract_id="orders_enriched",
            contract_version="1.1.0",
            dataset_name=None,
            dataset_version=dataset_version,
            run_type="observe",
            collect_examples=True,
            examples_limit=2,
            inputs={
                "orders": {
                    "dataset_version": "latest",
                    "status_strategy": {
                        "name": "allow-block",
                        "note": "Manual override: forced latest slice",
                        "target_status": "warn",
                    },
                }
            },
            output_adjustment="amplify-negative",
        )

        updated = pipeline.load_records()
        last = updated[-1]
        assert last.dataset_name == dataset_name
        assert last.dataset_version == dataset_version
        orders_details = last.dq_details.get("orders", {})
        overrides = orders_details.get("overrides", [])
        assert any("forced latest slice" in note for note in overrides)
        assert last.status in {"warning", "error"}
        output_details = last.dq_details.get("output", {})
        transformations = output_details.get("transformations", [])
        assert any("preserved negative input" in str(note) for note in transformations)
    finally:
        pipeline.save_records(original_records)
        if dq_dir.exists():
            shutil.rmtree(dq_dir)
        if backup.exists():
            shutil.copytree(backup, dq_dir)
        pipeline.set_active_version("orders", "2024-01-01")
        pipeline.set_active_version("orders__valid", "2025-09-28")
        pipeline.set_active_version("orders__reject", "2025-09-28")
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
            out_path = pipeline._resolve_output_path(contract, dataset_name, dataset_version)
            if out_path.exists():
                shutil.rmtree(out_path)
        SparkSession.builder.master("local[2]") \
            .appName("dc43-tests") \
            .config("spark.ui.enabled", "false") \
            .config("spark.sql.shuffle.partitions", "2") \
            .getOrCreate()
