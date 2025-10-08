import pytest

from dc43_demo_app.contracts_records import load_records, save_records
from dc43_demo_app.contracts_workspace import prepare_demo_workspace
from dc43_demo_app.streaming import run_streaming_scenario


try:  # pragma: no cover - optional dependency guard
    import pyspark  # type: ignore  # noqa: F401

    _PYSPARK_AVAILABLE = True
except ModuleNotFoundError:  # pragma: no cover - fallback when pyspark missing
    _PYSPARK_AVAILABLE = False


pytestmark = pytest.mark.skipif(not _PYSPARK_AVAILABLE, reason="pyspark required")


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
    original = load_records()
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
        reject_records = [
            r
            for r in load_records()
            if r.scenario_key == "streaming-dq-rejects"
            and r.dataset_name == "demo.streaming.events_rejects"
        ]
        assert reject_records, "expected reject dataset run"
        latest_reject = reject_records[-1]
        assert latest_reject.dataset_version
        assert latest_reject.run_type.endswith("rejects")
        reject_dir = _version_dir(
            workspace, "demo.streaming.events_rejects", latest_reject.dataset_version
        )
        assert reject_dir.exists()
        reject_marker = (reject_dir / ".dc43_version").read_text(encoding="utf-8").strip()
        assert reject_marker == latest_reject.dataset_version

        _, error_version = run_streaming_scenario(
            "streaming-schema-break", seconds=0, run_type="enforce"
        )
        error_records = [r for r in load_records() if r.scenario_key == "streaming-schema-break"]
        assert error_records
        assert error_records[-1].status == "error"
        assert error_records[-1].dataset_version == ""
        assert error_version == ""
    finally:
        save_records(original)
