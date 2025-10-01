import pytest
from fastapi.testclient import TestClient

from dc43_contracts_app import server as contracts_server
from dc43_demo_app import contracts_records as demo_records
from dc43_demo_app import server as demo_server
from dc43_demo_app.contracts_workspace import prepare_demo_workspace
from dc43_demo_app.scenarios import SCENARIOS

prepare_demo_workspace()
DatasetRecord = demo_records.DatasetRecord
load_records = demo_records.load_records
queue_flash = demo_records.queue_flash
save_records = demo_records.save_records
dq_version_records = demo_records.dq_version_records
scenario_run_rows = demo_records.scenario_run_rows
store = demo_records.get_store()

contracts_app = contracts_server.app
demo_app = demo_server.app


try:  # pragma: no cover - optional dependency in CI
    import pyspark  # type: ignore  # noqa: F401

    PYSPARK_AVAILABLE = True
except ModuleNotFoundError:  # pragma: no cover - fallback when pyspark missing
    PYSPARK_AVAILABLE = False


def test_contracts_page():
    client = TestClient(contracts_app)
    resp = client.get("/contracts")
    assert resp.status_code == 200


def test_contract_detail_page():
    rec = load_records()[0]
    client = TestClient(contracts_app)
    resp = client.get(f"/contracts/{rec.contract_id}/{rec.contract_version}")
    assert resp.status_code == 200
    assert 'id="access-tab"' in resp.text
    assert 'contract-data-panel' in resp.text


def test_contract_versions_page():
    rec = load_records()[0]
    client = TestClient(contracts_app)
    resp = client.get(f"/contracts/{rec.contract_id}")
    assert resp.status_code == 200
    assert "Open editor" in resp.text
    assert f"/contracts/{rec.contract_id}/{rec.contract_version}/edit" in resp.text


def test_contract_edit_form_renders_editor_sections():
    client = TestClient(contracts_app)
    resp = client.get("/contracts/orders/1.1.0/edit")
    assert resp.status_code == 200
    assert "Contract basics" in resp.text
    assert "Servers" in resp.text
    assert "Schema" in resp.text
    assert "Preview changes" in resp.text
    assert 'id="contract-data"' in resp.text


def test_new_contract_form_defaults():
    client = TestClient(contracts_app)
    resp = client.get("/contracts/new")
    assert resp.status_code == 200
    assert "Contract basics" in resp.text
    assert 'id="contract-data"' in resp.text
    assert '"version":"1.0.0"' in resp.text or '"version": "1.0.0"' in resp.text
    assert "Preview changes" in resp.text


def test_customers_contract_versions_page():
    client = TestClient(contracts_app)
    resp = client.get("/contracts/customers")
    assert resp.status_code == 200


def test_pipeline_runs_page_lists_scenarios():
    client = TestClient(demo_app)
    resp = client.get("/pipeline-runs")
    assert resp.status_code == 200
    for key, cfg in SCENARIOS.items():
        assert cfg["label"] in resp.text
        if cfg.get("description") or cfg.get("diagram"):
            assert f'data-scenario-popover="scenario-popover-{key}"' in resp.text


def test_dataset_detail_page():
    rec = load_records()[0]
    client = TestClient(contracts_app)
    resp = client.get(f"/datasets/{rec.dataset_name}/{rec.dataset_version}")
    assert resp.status_code == 200
    assert "order_id" in resp.text


def test_dataset_versions_page():
    rec = load_records()[0]
    client = TestClient(contracts_app)
    resp = client.get(f"/datasets/{rec.dataset_name}")
    assert resp.status_code == 200


def test_datasets_page_catalog_overview():
    client = TestClient(contracts_app)
    resp = client.get("/datasets")
    assert resp.status_code == 200
    assert "orders" in resp.text
    assert "Status:" in resp.text
    assert "Open editor" in resp.text


def test_dataset_pages_without_contract():
    original = load_records()
    record = DatasetRecord(
        contract_id="",
        contract_version="",
        dataset_name="missing-contract-dataset",
        dataset_version="2024-12-01",
        status="error",
        dq_details={},
        run_type="enforce",
        violations=0,
    )
    save_records([*original, record])
    client = TestClient(contracts_app)
    try:
        resp = client.get("/datasets")
        assert resp.status_code == 200
        assert "No contract" in resp.text

        resp_versions = client.get("/datasets/missing-contract-dataset")
        assert resp_versions.status_code == 200
        assert "No contract" in resp_versions.text

        resp_detail = client.get("/datasets/missing-contract-dataset/2024-12-01")
        assert resp_detail.status_code == 200
        assert "No contract recorded for this run" in resp_detail.text
    finally:
        save_records(original)


def test_flash_message_consumed_once():
    token = queue_flash(message="Hello there", error=None)
    client = TestClient(demo_app)

    first = client.get(f"/pipeline-runs?flash={token}")
    assert first.status_code == 200
    assert "Hello there" in first.text

    second = client.get(f"/pipeline-runs?flash={token}")
    assert second.status_code == 200
    assert "Hello there" not in second.text


def test_scenario_rows_default_mapping():
    rows = scenario_run_rows(load_records(), SCENARIOS)
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
        rows = scenario_run_rows(load_records(), SCENARIOS)
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
        rows = scenario_run_rows(load_records(), SCENARIOS)
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

    rows = scenario_run_rows(records, SCENARIOS)
    row_map = {row["key"]: row for row in rows}

    assert row_map["ok"]["latest"] is not None
    assert row_map["ok"]["latest"]["dataset_version"] == "2025-05-01T00:00:00Z"
    assert row_map["dq"]["latest"] is None
    assert row_map["schema-dq"]["latest"] is None


@pytest.mark.skipif(not PYSPARK_AVAILABLE, reason="pyspark required for preview API")
def test_contract_preview_api():
    rec = load_records()[0]
    client = TestClient(contracts_app)
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
    entries = dq_version_records(
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
    entries = dq_version_records(
        "orders",
        contract=contract,
        dataset_records=scoped_records,
    )
    versions = [entry["version"] for entry in entries]
    assert versions == ["2025-09-28"]
    statuses = {entry["version"]: entry["status"] for entry in entries}
    assert statuses["2025-09-28"] == "block"

