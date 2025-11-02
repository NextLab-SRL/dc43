from __future__ import annotations

import json
import shutil
from pathlib import Path
from typing import Any, Mapping

import pytest
from pyspark.sql import SparkSession

from dc43_demo_app import dlt_pipeline, pipeline
from dc43_demo_app.contracts_api import reset_governance_state
from dc43_demo_app.contracts_records import DatasetRecord
from dc43_integrations.spark.io import (
    ContractFirstDatasetLocator,
    ContractVersionLocator,
    StaticDatasetLocator,
)
from dc43_demo_app.contracts_workspace import current_workspace, prepare_demo_workspace
from dc43_demo_app.scenarios import SCENARIOS

prepare_demo_workspace()


@pytest.fixture(autouse=True)
def _reset_governance_state_fixture():
    reset_governance_state()
    yield
    reset_governance_state()


def _activate_scenario_versions(scenario_key: str) -> None:
    """Set ``latest`` aliases required for ``scenario_key``."""

    scenario = SCENARIOS.get(scenario_key)
    if not scenario:
        return
    for dataset, version in scenario.get("activate_versions", {}).items():
        try:
            pipeline.set_active_version(dataset, version)
        except FileNotFoundError:
            continue


def _run_dlt_for_params(params: Mapping[str, Any], *, scenario_key: str) -> tuple[str, str]:
    """Execute the demo pipeline in DLT mode using ``params``."""

    try:
        return dlt_pipeline.run_dlt_pipeline(
            params.get("contract_id"),
            params.get("contract_version"),
            params.get("dataset_name"),
            params.get("dataset_version"),
            params.get("run_type", "infer"),
            collect_examples=params.get("collect_examples", False),
            examples_limit=params.get("examples_limit", 5),
            violation_strategy=params.get("violation_strategy"),
            enforce_contract_status=params.get("enforce_contract_status"),
            inputs=params.get("inputs"),
            output_adjustment=params.get("output_adjustment"),
            data_product_flow=params.get("data_product_flow"),
            scenario_key=scenario_key,
        )
    except PermissionError as exc:  # pragma: no cover - environment quirk
        message = str(exc).lower()
        if "spark-submit" in message or "permission denied" in message:
            pytest.skip("Spark submit binary is not executable in this environment")
        raise


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
        assert "gt_amount" in message

        updated = pipeline.load_records()
        last = updated[-1]
        out = last.dq_details.get("output", {})
        fails = out.get("failed_expectations", {})

        assert "gt_amount" in fails
        assert fails["gt_amount"]["count"] > 0
        dq_status = out.get("dq_status", {})
        assert dq_status.get("status") in {"block", "warn", "error"}
        assert "reason" in dq_status or dq_status.get("status") == "block"
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


def test_demo_pipeline_existing_contract_ok(tmp_path: Path) -> None:
    workspace = current_workspace()

    def _active_version(dataset: str) -> str | None:
        latest = workspace.data_dir / dataset / "latest"
        if not latest.exists():
            return None
        try:
            resolved = latest.resolve()
        except OSError:
            resolved = latest
        if resolved.is_dir():
            marker = resolved / ".dc43_version"
            if marker.exists():
                try:
                    text = marker.read_text().strip()
                except OSError:
                    text = ""
                if text:
                    return text
            return resolved.name
        return None

    original_records = pipeline.load_records()
    dq_dir = Path(pipeline.DATASETS_FILE).parent / "dq_state"
    backup = tmp_path / "dq_state_backup_existing_ok"
    if dq_dir.exists():
        shutil.copytree(dq_dir, backup)
    existing_versions = set(pipeline.store.list_versions("orders_enriched"))

    dataset_name: str | None = None
    dataset_version: str | None = None
    original_orders_version = _active_version("orders")

    try:
        pipeline.set_active_version("orders", "2025-10-05")

        params = dict(SCENARIOS["ok"].get("params", {}))
        params.pop("mode", None)
        params.setdefault("dataset_name", None)
        params.setdefault("dataset_version", None)
        dataset_name, dataset_version = pipeline.run_pipeline(
            scenario_key="ok",
            **params,
        )

        assert dataset_name == "orders_enriched"

        updated = pipeline.load_records()
        last = updated[-1]
        assert last.dataset_name == dataset_name
        assert last.dataset_version == dataset_version
        assert last.status == "ok"
        output_section = last.dq_details.get("output", {})
        dq_status = output_section.get("dq_status") or {}
        assert dq_status.get("status") == "ok"
        assert not output_section.get("errors")
        orders_section = last.dq_details.get("orders") or {}
        orders_metrics = orders_section.get("metrics") or {}
        assert orders_metrics.get("row_count") == 3
        assert orders_metrics.get("violations.gt_amount", 0) == 0
    finally:
        if dq_dir.exists():
            shutil.rmtree(dq_dir)
        if backup.exists():
            shutil.copytree(backup, dq_dir)
        if original_orders_version:
            pipeline.set_active_version("orders", original_orders_version)
        else:
            pipeline.set_active_version("orders", "2024-01-01")
        new_versions = set(pipeline.store.list_versions("orders_enriched")) - existing_versions
        for ver in new_versions:
            draft_path = Path(pipeline.store.base_path) / "orders_enriched" / f"{ver}.json"
            if draft_path.exists():
                draft_path.unlink()
        if dataset_name and dataset_version:
            out_dir = Path(pipeline.DATA_DIR) / dataset_name / dataset_version
            if out_dir.exists():
                shutil.rmtree(out_dir, ignore_errors=True)


DLT_SCENARIO_EXPECTATIONS = [
    (
        "ok",
        {
            "status": "ok",
            "dataset": "orders_enriched",
            "dlt_asset": "orders_enriched",
        },
    ),
    (
        "ok-dlt",
        {
            "status": "ok",
            "dataset": "orders_enriched",
            "dlt_asset": "orders_enriched",
        },
    ),
    (
        "dq",
        {
            "error": "DQ violation",
            "status": "error",
            "dataset": "orders_enriched",
            "failed_expectations": True,
        },
    ),
    (
        "schema-dq",
        {
            "error": "Schema validation failed",
            "status": "error",
            "dataset": "orders_enriched",
            "failed_expectations": True,
            "schema_errors": True,
        },
    ),
    (
        "contract-draft-block",
        {
            "error": "draft",
            "status": "error",
            "dataset": "orders_enriched",
            "contract_status_error": True,
            "contract_policy_allowed": ["active"],
        },
    ),
    (
        "contract-draft-override",
        {
            "status": "ok",
            "dataset": "orders_enriched",
            "contract_policy_allowed": ["active", "draft"],
        },
    ),
    (
        "read-invalid-block",
        {
            "error": "DQ status is blocking",
            "status": "error",
            "dataset": "orders_enriched",
            "dq_reason_prefix": "DQ status is blocking",
        },
    ),
    (
        "read-valid-subset",
        {
            "status": "ok",
            "dataset": "orders_enriched",
            "dataset_version": "valid-ok",
        },
    ),
    (
        "read-valid-subset-violation",
        {
            "status": "error",
            "dataset": "orders_enriched",
            "dataset_version": "valid-invalid",
            "failed_expectations": True,
            "raises_value_error": True,
        },
    ),
    (
        "read-override-full",
        {
            "status_in": {"warning", "error"},
            "dataset": "orders_enriched",
            "dataset_version": "override-full",
        },
    ),
    (
        "split-lenient",
        {
            "status": "warning",
            "dataset": "orders_enriched",
            "auxiliary_kinds": {"valid", "reject"},
        },
    ),
    (
        "data-product-roundtrip",
        {
            "status": "ok",
            "dataset": "orders_enriched",
        },
    ),
]


@pytest.mark.parametrize(
    "scenario_key, expectation",
    DLT_SCENARIO_EXPECTATIONS,
    ids=[entry[0] for entry in DLT_SCENARIO_EXPECTATIONS],
)
def test_demo_pipeline_dlt_scenarios(
    scenario_key: str,
    expectation: Mapping[str, Any],
) -> None:
    prepare_demo_workspace()
    _activate_scenario_versions(scenario_key)
    params = dict(SCENARIOS[scenario_key].get("params", {}))
    params.pop("mode", None)

    dataset_result: tuple[str, str] | None = None
    try:
        should_raise = expectation.get("raises_value_error") or expectation.get("error")
        if should_raise:
            with pytest.raises(ValueError) as excinfo:
                _run_dlt_for_params(params, scenario_key=scenario_key)
            error_text = expectation.get("error")
            if error_text:
                assert error_text.lower() in str(excinfo.value).lower()
        else:
            dataset_result = _run_dlt_for_params(params, scenario_key=scenario_key)
            expected_dataset = expectation.get("dataset")
            if expected_dataset and dataset_result:
                assert dataset_result[0] == expected_dataset
            expected_version = expectation.get("dataset_version")
            if expected_version and dataset_result:
                assert dataset_result[1] == expected_version

        records = pipeline.load_records()
        assert records, "expected the run to append a dataset record"
        last = records[-1]
        assert last.scenario_key == scenario_key
        output_details = (
            last.dq_details.get("output", {})
            if isinstance(last.dq_details, Mapping)
            else {}
        )
        assert output_details.get("pipeline_engine") == "dlt"
        assert output_details.get("dlt_module_name")
        assert output_details.get("dlt_module_stub") in {True, False}
        reports = output_details.get("dlt_expectations")
        assert isinstance(reports, list)
        assert reports
        assert all(isinstance(entry, Mapping) for entry in reports)
        asset_hint = (
            expectation.get("dlt_asset")
            or expectation.get("dataset")
            or params.get("dataset_name")
            or params.get("contract_id")
        )
        if asset_hint:
            assert all(entry.get("asset") == asset_hint for entry in reports)

        if "status" in expectation:
            assert last.status == expectation["status"]
        if "status_in" in expectation:
            assert last.status in expectation["status_in"]
        if expectation.get("failed_expectations"):
            failures = output_details.get("failed_expectations", {})
            assert failures
        if expectation.get("schema_errors"):
            errors = output_details.get("errors", [])
            assert errors
        if expectation.get("dq_reason_prefix"):
            dq_summary = output_details.get("dq_status") or {}
            reason = str(dq_summary.get("reason", ""))
            assert reason.startswith(expectation["dq_reason_prefix"])
        if expectation.get("contract_status_error"):
            assert output_details.get("contract_status_error")
        if expectation.get("contract_policy_allowed"):
            policy = output_details.get("contract_status_policy") or {}
            assert policy.get("allowed") == expectation["contract_policy_allowed"]
        if expectation.get("dataset_version"):
            assert last.dataset_version == expectation["dataset_version"]
        if expectation.get("auxiliary_kinds"):
            aux_entries = output_details.get("auxiliary_datasets", []) or []
            kinds = {
                entry.get("kind")
                for entry in aux_entries
                if isinstance(entry, Mapping)
            }
            assert expectation["auxiliary_kinds"].issubset(kinds)
    finally:
        prepare_demo_workspace()


def test_data_product_input_locator_defaults_to_latest() -> None:
    locator = pipeline._data_product_input_locator({})
    assert isinstance(locator, ContractVersionLocator)
    assert locator.dataset_version == "latest"
    assert isinstance(locator.base, ContractFirstDatasetLocator)


def test_data_product_input_locator_respects_version_and_id() -> None:
    locator = pipeline._data_product_input_locator(
        {"dataset_version": "2025-10-05", "dataset_id": "orders"}
    )
    assert isinstance(locator, ContractVersionLocator)
    assert locator.dataset_version == "2025-10-05"
    assert locator.dataset_id == "orders"


def test_data_product_input_locator_accepts_custom_locator() -> None:
    custom = StaticDatasetLocator(
        dataset_id="orders",
        dataset_version="custom",
        base=ContractFirstDatasetLocator(),
    )
    locator = pipeline._data_product_input_locator({"dataset_locator": custom})
    assert locator is custom


def test_data_product_input_locator_prefers_latest_ok_record() -> None:
    records = [
        DatasetRecord(
            contract_id="orders",
            contract_version="1.1.0",
            dataset_name="orders",
            dataset_version="2025-09-28",
            status="error",
        ),
        DatasetRecord(
            contract_id="orders",
            contract_version="1.1.0",
            dataset_name="orders",
            dataset_version="2025-10-05",
            status="ok",
        ),
    ]

    locator = pipeline._data_product_input_locator({"dataset_id": "orders"}, records=records)

    assert isinstance(locator, ContractVersionLocator)
    assert locator.dataset_version == "2025-10-05"


def test_run_pipeline_refreshes_aliases_for_data_product_flow(monkeypatch) -> None:
    refreshed: list[str | None] = []

    def fake_refresh(dataset: str | None = None) -> None:
        refreshed.append(dataset)

    def fake_run_flow(**_: object) -> tuple[str, str]:
        return "dp.analytics.stage", "20251005T065105464905Z"

    monkeypatch.setattr(pipeline.contracts_server, "refresh_dataset_aliases", fake_refresh)
    monkeypatch.setattr(pipeline, "_run_data_product_flow", fake_run_flow)

    dataset, version = pipeline.run_pipeline(
        contract_id=None,
        contract_version=None,
        dataset_name=None,
        dataset_version=None,
        run_type="observe",
        data_product_flow={"input": {}, "output": {}},
    )

    assert dataset == "dp.analytics.stage"
    assert version == "20251005T065105464905Z"
    assert refreshed == [None]


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


def test_demo_pipeline_blocks_draft_contract(tmp_path: Path) -> None:
    original_records = pipeline.load_records()
    dq_dir = Path(pipeline.DATASETS_FILE).parent / "dq_state"
    backup = tmp_path / "dq_state_backup_draft_block"
    if dq_dir.exists():
        shutil.copytree(dq_dir, backup)
    existing_versions = set(pipeline.store.list_versions("orders_enriched"))

    try:
        with pytest.raises(ValueError) as excinfo:
            pipeline.run_pipeline(
                contract_id="orders_enriched",
                contract_version="3.0.0",
                dataset_name=None,
                dataset_version=None,
                run_type="enforce",
            )

        message = str(excinfo.value)
        assert "status" in message.lower()
        assert "draft" in message.lower()

        updated = pipeline.load_records()
        last = updated[-1]
        output = last.dq_details.get("output", {})
        assert output.get("contract_status_error")
        policy = output.get("contract_status_policy", {})
        assert policy.get("allowed") == ["active"]
    finally:
        if dq_dir.exists():
            shutil.rmtree(dq_dir)
        if backup.exists():
            shutil.copytree(backup, dq_dir)
        new_versions = set(pipeline.store.list_versions("orders_enriched")) - existing_versions
        for ver in new_versions:
            draft_path = Path(pipeline.store.base_path) / "orders_enriched" / f"{ver}.json"
            if draft_path.exists():
                draft_path.unlink()
            out_dir = Path(pipeline.DATA_DIR) / "orders_enriched" / ver
            if out_dir.exists():
                shutil.rmtree(out_dir, ignore_errors=True)
        SparkSession.builder.master("local[2]") \
            .appName("dc43-tests") \
            .config("spark.ui.enabled", "false") \
            .config("spark.sql.shuffle.partitions", "2") \
            .getOrCreate()


def test_demo_pipeline_allows_draft_contract_with_override(tmp_path: Path) -> None:
    original_records = pipeline.load_records()
    dq_dir = Path(pipeline.DATASETS_FILE).parent / "dq_state"
    backup = tmp_path / "dq_state_backup_draft_override"
    if dq_dir.exists():
        shutil.copytree(dq_dir, backup)
    existing_versions = set(pipeline.store.list_versions("orders_enriched"))

    dataset_name: str | None = None
    dataset_version: str | None = None
    try:
        dataset_name, dataset_version = pipeline.run_pipeline(
            contract_id="orders_enriched",
            contract_version="3.0.0",
            dataset_name=None,
            dataset_version=None,
            run_type="enforce",
            violation_strategy={
                "name": "default",
                "contract_status": {
                    "allowed_contract_statuses": ["active", "draft"],
                    "allow_missing_contract_status": False,
                },
            },
            output_adjustment="boost-amounts",
            inputs={
                "orders": {
                    "dataset_id": "orders::valid",
                    "dataset_version": "latest__valid",
                }
            },
        )

        updated = pipeline.load_records()
        last = updated[-1]
        output = last.dq_details.get("output", {})
        policy = output.get("contract_status_policy", {})
        assert policy.get("allowed") == ["active", "draft"]
        assert output.get("contract_status_enforced") is True
        assert last.contract_version == "3.0.0"
        assert last.status == "ok"
        assert (output.get("dq_status") or {}).get("status") == "ok"
        transformations = output.get("transformations", [])
        assert any("raised low order amounts" in note for note in transformations)
    finally:
        if dq_dir.exists():
            shutil.rmtree(dq_dir)
        if backup.exists():
            shutil.copytree(backup, dq_dir)
        new_versions = set(pipeline.store.list_versions("orders_enriched")) - existing_versions
        for ver in new_versions:
            draft_path = Path(pipeline.store.base_path) / "orders_enriched" / f"{ver}.json"
            if draft_path.exists():
                draft_path.unlink()
        if dataset_name and dataset_version:
            out_dir = Path(pipeline.DATA_DIR) / dataset_name / dataset_version
            if out_dir.exists():
                shutil.rmtree(out_dir, ignore_errors=True)


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
