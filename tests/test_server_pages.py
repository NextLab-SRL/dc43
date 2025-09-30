import pytest
from fastapi.testclient import TestClient

from dc43.demo_app.server import (
    SCENARIOS,
    DatasetRecord,
    _dq_version_records,
    app,
    load_records,
    queue_flash,
    save_records,
    scenario_run_rows,
    store,
)


try:  # pragma: no cover - optional dependency in CI
    import pyspark  # type: ignore  # noqa: F401

    PYSPARK_AVAILABLE = True
except ModuleNotFoundError:  # pragma: no cover - fallback when pyspark missing
    PYSPARK_AVAILABLE = False


def test_contract_routes_require_portal_configuration():
    client = TestClient(app)
    for path in [
        "/contracts",
        "/contracts/orders",
        "/contracts/orders/1.0.0",
        "/contracts/orders/1.1.0/edit",
        "/contracts/new",
    ]:
        response = client.get(path)
        assert response.status_code == 404


def test_dataset_routes_require_portal_configuration():
    client = TestClient(app)
    for path in [
        "/datasets",
        "/datasets/orders",
        "/datasets/orders/2024-01-01",
    ]:
        response = client.get(path)
        assert response.status_code == 404


def test_pipeline_runs_page_lists_scenarios():
    client = TestClient(app)
    resp = client.get("/pipeline-runs")
    assert resp.status_code == 200
    for key, cfg in SCENARIOS.items():
        assert cfg["label"] in resp.text
        if cfg.get("description") or cfg.get("diagram"):
            assert f'data-scenario-popover="scenario-popover-{key}"' in resp.text


def test_datasets_page_catalog_overview_removed():
    client = TestClient(app)
    resp = client.get("/datasets")
    assert resp.status_code == 404


def test_flash_message_consumed_once():
    token = queue_flash(message="Hello there", error=None)
    client = TestClient(app)

    first = client.get(f"/pipeline-runs?flash={token}")
    assert first.status_code == 200
    assert "Hello there" in first.text

    second = client.get(f"/pipeline-runs?flash={token}")
    assert second.status_code == 200
    assert "Hello there" not in second.text


def test_scenario_rows_default_mapping():
    rows = scenario_run_rows(load_records())
    assert len(rows) == len(SCENARIOS)
    row_map = {row["key"]: row for row in rows}

    no_contract = row_map.get("no-contract")
    assert no_contract is not None
    assert no_contract["dataset_name"] == "result-no-existing-contract"
    assert no_contract["latest"] is None

    ok_row = row_map.get("ok")
    assert ok_row is not None
    assert ok_row["dataset_name"] == "orders_enriched"
    assert ok_row["run_count"] == 0
    assert ok_row["latest"] is None


def test_scenario_rows_tracks_latest_record():
    original = load_records()
    extra_records = [
        DatasetRecord(
            contract_id="orders_enriched",
            contract_version="1.0.0",
            dataset_name="orders_enriched",
            dataset_version="2024-12-01T00:00:00Z",
            status="ok",
            dq_details={"output": {"violations": 0}},
            run_type="enforce",
            violations=0,
            scenario_key="ok",
        ),
        DatasetRecord(
            contract_id="orders_enriched",
            contract_version="1.0.0",
            dataset_name="orders_enriched",
            dataset_version="2024-12-02T00:00:00Z",
            status="warning",
            dq_details={"output": {"violations": 1}},
            run_type="enforce",
            violations=1,
            scenario_key="ok",
        ),
    ]
    try:
        save_records([*original, *extra_records])
        rows = scenario_run_rows(load_records())
        row_map = {row["key"]: row for row in rows}
        ok_row = row_map["ok"]
        base_runs = len([rec for rec in original if rec.scenario_key == "ok"])
        assert ok_row["run_count"] == base_runs + len(extra_records)
        assert ok_row["latest"] is not None
        assert ok_row["latest"]["dataset_version"] == "2024-12-02T00:00:00Z"
        assert ok_row["latest"]["status"] == "warning"
    finally:
        save_records(original)


def test_scenario_rows_isolate_runs_per_scenario():
    original = load_records()
    scenario_records = [
        DatasetRecord(
            contract_id="orders_enriched",
            contract_version="1.1.0",
            dataset_name="orders_enriched",
            dataset_version="2025-01-01T00:00:00Z",
            status="ok",
            dq_details={"output": {"violations": 0}},
            run_type="observe",
            violations=0,
            scenario_key="split-lenient",
        ),
        DatasetRecord(
            contract_id="orders_enriched",
            contract_version="1.1.0",
            dataset_name="orders_enriched",
            dataset_version="2025-01-02T00:00:00Z",
            status="ok",
            dq_details={"output": {"violations": 0}},
            run_type="enforce",
            violations=0,
            scenario_key="ok",
        ),
    ]
    try:
        save_records([*original, *scenario_records])
        rows = scenario_run_rows(load_records())
        row_map = {row["key"]: row for row in rows}
        split_row = row_map["split-lenient"]
        ok_row = row_map["ok"]

        existing_split = len([rec for rec in original if rec.scenario_key == "split-lenient"])
        existing_ok = len([rec for rec in original if rec.scenario_key == "ok"])

        assert split_row["run_count"] == existing_split + 1
        assert ok_row["run_count"] == existing_ok + 1
        assert split_row["latest"]
        assert ok_row["latest"]
        assert split_row["latest"]["scenario_key"] == "split-lenient"
        assert ok_row["latest"]["scenario_key"] == "ok"
        assert split_row["latest"]["dataset_version"] == "2025-01-01T00:00:00Z"
        assert ok_row["latest"]["dataset_version"] == "2025-01-02T00:00:00Z"
    finally:
        save_records(original)


def test_scenario_rows_ignore_mismatched_scenario_runs():
    records = [
        DatasetRecord(
            contract_id="orders_enriched",
            contract_version="1.0.0",
            dataset_name="orders_enriched",
            dataset_version="2025-05-01T00:00:00Z",
            status="ok",
            dq_details={"output": {"violations": 0}},
            run_type="enforce",
            violations=0,
            scenario_key="ok",
        )
    ]

    rows = scenario_run_rows(records)
    row_map = {row["key"]: row for row in rows}

    assert row_map["ok"]["latest"] is not None
    assert row_map["ok"]["latest"]["dataset_version"] == "2025-05-01T00:00:00Z"
    assert row_map["dq"]["latest"] is None
    assert row_map["schema-dq"]["latest"] is None


@pytest.mark.skipif(not PYSPARK_AVAILABLE, reason="pyspark required for preview API")
def test_contract_preview_api():
    rec = load_records()[0]
    client = TestClient(app)
    resp = client.get(f"/api/contracts/{rec.contract_id}/{rec.contract_version}/preview")
    assert resp.status_code == 200
    payload = resp.json()
    assert payload.get("dataset_version")
    assert isinstance(payload.get("rows"), list)
    assert isinstance(payload.get("known_versions"), list)


def test_dq_version_records_scoped_to_contract_runs():
    contract = store.get("orders", "1.0.0")
    scoped_records = [
        record
        for record in load_records()
        if record.contract_id == "orders" and record.contract_version == "1.0.0"
    ]
    entries = _dq_version_records(
        "orders",
        contract=contract,
        dataset_records=scoped_records,
    )
    versions = [entry["version"] for entry in entries]
    assert versions == ["2024-01-01"]
    statuses = {entry["version"]: entry["status"] for entry in entries}
    assert statuses["2024-01-01"] == "ok"


def test_dq_version_records_excludes_other_contract_versions():
    contract = store.get("orders", "1.1.0")
    scoped_records = [
        record
        for record in load_records()
        if record.contract_id == "orders" and record.contract_version == "1.1.0"
    ]
    entries = _dq_version_records(
        "orders",
        contract=contract,
        dataset_records=scoped_records,
    )
    versions = [entry["version"] for entry in entries]
    assert versions == ["2025-09-28"]
    statuses = {entry["version"]: entry["status"] for entry in entries}
    assert statuses["2025-09-28"] == "block"

