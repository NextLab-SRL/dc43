import pytest

from pathlib import Path

from dc43_demo_app.contracts_api import reset_governance_state
from dc43_demo_app.contracts_records import load_records
from dc43_demo_app.contracts_workspace import prepare_demo_workspace
from dc43_demo_app.streaming import run_streaming_scenario


try:  # pragma: no cover - optional dependency guard
    import pyspark  # type: ignore  # noqa: F401

    _PYSPARK_AVAILABLE = True
except ModuleNotFoundError:  # pragma: no cover - fallback when pyspark missing
    _PYSPARK_AVAILABLE = False


pytestmark = pytest.mark.skipif(not _PYSPARK_AVAILABLE, reason="pyspark required")


@pytest.fixture(autouse=True)
def _reset_streaming_governance():
    reset_governance_state()
    yield
    reset_governance_state()


def _version_dir(workspace, dataset: str, version: str):
    root = workspace.data_dir / dataset
    candidate = root / version
    if candidate.exists():
        return candidate
    safe = "".join(
        ch if ch.isalnum() or ch in {"-", "_", "."} else "_" for ch in version
    )
    return root / safe


def test_streaming_scenarios_record_dataset_runs():
    workspace, _ = prepare_demo_workspace()
    try:
        dataset, version = run_streaming_scenario("streaming-valid", seconds=0, run_type="observe")
        assert dataset == "demo.streaming.events_processed"
        assert version
        records = [r for r in load_records() if r.scenario_key == "streaming-valid"]
        assert records
        primary = [r for r in records if r.run_type == "observe"]
        assert primary, "expected primary streaming record"
        latest_record = primary[-1]
        assert latest_record.status == "ok"
        timeline = latest_record.dq_details.get("timeline") if latest_record.dq_details else []
        assert timeline, "expected streaming timeline entries"
        validation_event = next(
            (event for event in timeline if event.get("phase") == "Validation"),
            None,
        )
        assert validation_event
        metrics = validation_event.get("metrics") or {}
        assert metrics.get("row_count", 0) > 0
        batches = latest_record.dq_details.get("output", {}).get("streaming_batches")
        assert batches
        assert any((batch.get("row_count", 0) or 0) > 0 for batch in batches)
        batch_entries = [r for r in records if r.run_type.endswith("-batch")]
        assert batch_entries, "expected micro-batch records"

        processed_dir = _version_dir(workspace, dataset, version)
        assert processed_dir.exists()
        marker = (processed_dir / ".dc43_version").read_text(encoding="utf-8").strip()
        assert marker == version

        input_details = latest_record.dq_details.get("input", {}) if latest_record.dq_details else {}
        input_version = input_details.get("dataset_version")
        assert input_version
        input_dir = _version_dir(workspace, "demo.streaming.events", input_version)
        assert input_dir.exists()
        input_marker = (input_dir / ".dc43_version").read_text(encoding="utf-8").strip()
        assert input_marker == input_version
        input_records = [
            r
            for r in records
            if r.dataset_name == "demo.streaming.events" and r.dataset_version == input_version
        ]
        assert input_records, "expected input dataset run to be recorded"
        assert input_records[-1].run_type.endswith("input")

        _, warning_version = run_streaming_scenario(
            "streaming-dq-rejects", seconds=0, run_type="observe"
        )
        assert warning_version
        all_records = load_records()
        reject_dataset_runs = [
            r
            for r in all_records
            if r.scenario_key == "streaming-dq-rejects"
            and r.dataset_name == "demo.streaming.events_rejects"
        ]
        assert (
            not reject_dataset_runs
        ), "reject sink should remain ungoverned and avoid dataset records"
        warning_records = [
            r
            for r in all_records
            if r.scenario_key == "streaming-dq-rejects"
            and r.dataset_name == "demo.streaming.events_processed"
            and r.run_type == "observe"
        ]
        assert warning_records, "expected processed dataset warning run"
        latest_warning = warning_records[-1]
        reject_section = latest_warning.dq_details.get("rejects", {})
        assert isinstance(reject_section, dict)
        assert reject_section.get("dataset_id") == "demo.streaming.events_rejects"
        assert reject_section.get("dataset_version") == latest_warning.dataset_version
        assert reject_section.get("governed") is False
        reject_path = reject_section.get("path")
        assert isinstance(reject_path, str) and reject_path
        reject_dir = Path(reject_path)
        assert reject_dir.exists() and reject_dir.is_dir()
        reject_marker = (reject_dir / ".dc43_version").read_text(encoding="utf-8").strip()
        assert reject_marker == latest_warning.dataset_version
        if reject_section.get("row_count", 0):
            assert any(
                (batch.get("violations", 0) or 0) > 0
                for batch in reject_section.get("streaming_batches", [])
                if isinstance(batch, dict)
            )

        processed_batches = latest_record.dq_details.get("output", {}).get(
            "streaming_batches", []
        )
        assert any((batch.get("violations", 0) or 0) == 0 for batch in processed_batches)
        assert any((batch.get("violations", 0) or 0) > 0 for batch in processed_batches)
        assert "warning" in {batch.get("status") for batch in processed_batches}
        assert "success" in {batch.get("status") for batch in processed_batches}

        _, error_version = run_streaming_scenario(
            "streaming-schema-break", seconds=0, run_type="enforce"
        )
        error_records = [r for r in load_records() if r.scenario_key == "streaming-schema-break"]
        assert error_records
        assert error_records[-1].status == "error"
        assert error_records[-1].dataset_version == ""
        assert error_version == ""
    finally:
        reset_governance_state()
