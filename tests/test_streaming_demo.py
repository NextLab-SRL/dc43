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


def test_streaming_scenarios_record_dataset_runs():
    prepare_demo_workspace()
    original = load_records()
    try:
        dataset, version = run_streaming_scenario("streaming-valid", seconds=2, run_type="observe")
        assert dataset == "demo.streaming.events_processed"
        assert version
        records = [r for r in load_records() if r.scenario_key == "streaming-valid"]
        assert records
        assert records[-1].status == "ok"
        timeline = records[-1].dq_details.get("timeline") if records[-1].dq_details else []
        assert timeline, "expected streaming timeline entries"
        validation_event = next(
            (event for event in timeline if event.get("phase") == "Validation"),
            None,
        )
        assert validation_event
        metrics = validation_event.get("metrics") or {}
        assert metrics.get("row_count", 0) > 0
        batches = records[-1].dq_details.get("output", {}).get("streaming_batches")
        assert batches
        assert any((batch.get("row_count", 0) or 0) > 0 for batch in batches)

        _, error_version = run_streaming_scenario("streaming-schema-break", seconds=0, run_type="enforce")
        error_records = [r for r in load_records() if r.scenario_key == "streaming-schema-break"]
        assert error_records
        assert error_records[-1].status == "error"
        assert error_records[-1].dataset_version == ""
        assert error_version == ""
    finally:
        save_records(original)
