import asyncio
import json
from typing import Any, Iterable

import pytest
from fastapi.testclient import TestClient

from dc43_contracts_app import server as contracts_server
from dc43_demo_app import contracts_records as demo_records
from dc43_demo_app import pipeline as demo_pipeline
from dc43_demo_app import server as demo_server
from dc43_demo_app.contracts_api import reset_governance_state
from dc43_demo_app.contracts_workspace import prepare_demo_workspace
from dc43_demo_app.scenarios import SCENARIOS

prepare_demo_workspace()
DatasetRecord = demo_records.DatasetRecord
load_records = demo_records.load_records
queue_flash = demo_records.queue_flash
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


@pytest.fixture(autouse=True)
def _reset_governance():
    reset_governance_state()
    yield
    reset_governance_state()


def _ensure_sample_records(
    *,
    runs: Iterable[tuple[str | None, str | None, str | None, str | None, str]] = (),
) -> list[DatasetRecord]:
    records = load_records()
    if records:
        return records

    if not PYSPARK_AVAILABLE:
        pytest.skip("pyspark required to generate dataset history")

    scenarios = list(runs) or [
        ("orders_enriched", "1.1.0", None, None, "observe"),
    ]
    for contract_id, contract_version, dataset_name, dataset_version, run_type in scenarios:
        demo_pipeline.run_pipeline(
            contract_id=contract_id,
            contract_version=contract_version,
            dataset_name=dataset_name,
            dataset_version=dataset_version,
            run_type=run_type,
            collect_examples=False,
            examples_limit=1,
        )
    return load_records()


def test_contracts_page():
    client = TestClient(contracts_app)
    resp = client.get("/contracts")
    assert resp.status_code == 200


def test_contract_detail_page():
    rec = _ensure_sample_records()[0]
    client = TestClient(contracts_app)
    resp = client.get(f"/contracts/{rec.contract_id}/{rec.contract_version}")
    assert resp.status_code == 200
    assert 'id="access-tab"' in resp.text
    assert 'contract-data-panel' in resp.text


def test_contract_versions_page():
    rec = _ensure_sample_records()[0]
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
    assert "Run with Databricks DLT" in resp.text


@pytest.mark.skipif(not PYSPARK_AVAILABLE, reason="pyspark required")
def test_dataset_detail_page():
    rec = _ensure_sample_records()[0]
    client = TestClient(contracts_app)
    resp = client.get(f"/datasets/{rec.dataset_name}/{rec.dataset_version}")
    assert resp.status_code == 200
    assert "order_id" in resp.text
    assert "datasetMetricsChart" not in resp.text


@pytest.mark.skipif(not PYSPARK_AVAILABLE, reason="pyspark required")
def test_dataset_versions_page():
    rec = _ensure_sample_records()[0]
    client = TestClient(contracts_app)
    resp = client.get(f"/datasets/{rec.dataset_name}")
    assert resp.status_code == 200
    assert "datasetMetricsChart" in resp.text
    assert 'data-table-sort="dataset-history"' in resp.text


@pytest.mark.skipif(not PYSPARK_AVAILABLE, reason="pyspark required")
def test_datasets_page_catalog_overview():
    _ensure_sample_records()
    client = TestClient(contracts_app)
    resp = client.get("/datasets")
    assert resp.status_code == 200
    assert "orders" in resp.text
    assert "Status:" in resp.text
    assert "Open editor" in resp.text


@pytest.mark.skipif(not PYSPARK_AVAILABLE, reason="pyspark required")
def test_dataset_pages_without_contract():
    demo_pipeline.run_pipeline(
        contract_id=None,
        contract_version=None,
        dataset_name="missing-contract-dataset",
        dataset_version="2024-12-01",
        run_type="enforce",
        collect_examples=False,
        examples_limit=1,
    )
    client = TestClient(contracts_app)

    resp = client.get("/datasets")
    assert resp.status_code == 200
    assert "No contract" in resp.text

    resp_versions = client.get("/datasets/missing-contract-dataset")
    assert resp_versions.status_code == 200
    assert "No contract" in resp_versions.text

    resp_detail = client.get("/datasets/missing-contract-dataset/2024-12-01")
    assert resp_detail.status_code == 200
    assert "No contract recorded for this run" in resp_detail.text


def test_flash_message_consumed_once():
    token = queue_flash(message="Hello there", error=None)
    client = TestClient(demo_app)

    first = client.get(f"/pipeline-runs?flash={token}")
    assert first.status_code == 200
    assert "Hello there" in first.text

    second = client.get(f"/pipeline-runs?flash={token}")
    assert second.status_code == 200
    assert "Hello there" not in second.text


def test_run_pipeline_endpoint_passes_data_product_flow(monkeypatch):
    params = SCENARIOS["data-product-roundtrip"]["params"]
    captured: dict[str, Any] = {}

    def fake_run_pipeline(*args: Any, **kwargs: Any) -> tuple[str, str]:
        captured["args"] = args
        captured["kwargs"] = kwargs
        return (
            kwargs.get("dataset_name") or params.get("dataset_name") or "orders_enriched",
            "2024-01-01T00:00:00Z",
        )

    async def fake_to_thread(func: Any, /, *args: Any, **kwargs: Any) -> Any:
        return func(*args, **kwargs)

    monkeypatch.setattr("dc43_demo_app.pipeline.run_pipeline", fake_run_pipeline)
    monkeypatch.setattr(asyncio, "to_thread", fake_to_thread)

    client = TestClient(demo_app)
    resp = client.post(
        "/pipeline/run",
        data={"scenario": "data-product-roundtrip"},
        headers={"accept": "application/json"},
    )

    assert resp.status_code == 200
    payload = resp.json()
    assert payload["status"] == "success"
    assert payload["mode"] == "pipeline"
    assert payload["message"].startswith("Run succeeded: ")
    assert payload["detail_url"].startswith("/pipeline-runs/data-product-roundtrip?flash=")
    assert captured["args"][:5] == (
        params.get("contract_id"),
        params.get("contract_version"),
        params.get("dataset_name"),
        params.get("dataset_version"),
        params.get("run_type", "infer"),
    )
    kwargs = captured["kwargs"]
    assert kwargs["collect_examples"] == params.get("collect_examples", False)
    assert kwargs["examples_limit"] == params.get("examples_limit", 5)
    assert kwargs["violation_strategy"] == params.get("violation_strategy")
    assert kwargs["enforce_contract_status"] == params.get("enforce_contract_status")
    assert kwargs["inputs"] == params.get("inputs")
    assert kwargs["output_adjustment"] == params.get("output_adjustment")
    assert kwargs["data_product_flow"] == params.get("data_product_flow")
    assert kwargs["scenario_key"] == "data-product-roundtrip"


def test_run_pipeline_endpoint_redirects_without_json(monkeypatch):
    params = SCENARIOS["ok"]["params"]

    def fake_run_pipeline(*args: Any, **kwargs: Any) -> tuple[str, str]:
        return (params.get("dataset_name") or "orders_enriched", "2024-03-01T00:00:00Z")

    async def fake_to_thread(func: Any, /, *args: Any, **kwargs: Any) -> Any:
        return func(*args, **kwargs)

    monkeypatch.setattr("dc43_demo_app.pipeline.run_pipeline", fake_run_pipeline)
    monkeypatch.setattr(asyncio, "to_thread", fake_to_thread)

    client = TestClient(demo_app)
    resp = client.post(
        "/pipeline/run",
        data={"scenario": "ok"},
    )

    assert resp.status_code == 302
    assert resp.headers["location"].startswith("/pipeline-runs/ok?flash=")


@pytest.mark.skipif(not PYSPARK_AVAILABLE, reason="pyspark required")
def test_pipeline_runs_endpoint_populates_flash(monkeypatch):
    params = SCENARIOS["ok"]["params"]
    captured: dict[str, Any] = {}

    def fake_run_pipeline(*args: Any, **kwargs: Any) -> tuple[str, str]:
        captured["args"] = args
        captured["kwargs"] = kwargs
        return (params.get("dataset_name") or "orders_enriched", "2024-02-01T00:00:00Z")

    async def fake_to_thread(func: Any, /, *args: Any, **kwargs: Any) -> Any:
        return func(*args, **kwargs)

    monkeypatch.setattr("dc43_demo_app.pipeline.run_pipeline", fake_run_pipeline)
    monkeypatch.setattr(asyncio, "to_thread", fake_to_thread)

    client = TestClient(demo_app)
    resp = client.post(
        "/pipeline/run",
        data={"scenario": "ok"},
        headers={"accept": "application/json"},
    )

    assert resp.status_code == 200
    payload = resp.json()
    assert payload["status"] == "success"
    assert payload["mode"] == "pipeline"
    assert payload["message"].startswith("Run succeeded: ")
    assert payload["detail_url"].startswith("/pipeline-runs/ok?flash=")
    assert captured["args"][:5] == (
        params.get("contract_id"),
        params.get("contract_version"),
        params.get("dataset_name"),
        params.get("dataset_version"),
        params.get("run_type", "infer"),
    )


@pytest.mark.skipif(not PYSPARK_AVAILABLE, reason="pyspark required")
def test_pipeline_runs_page_renders_details():
    _ensure_sample_records()
    client = TestClient(demo_app)
    resp = client.get("/pipeline-runs")
    assert resp.status_code == 200
    assert "orders" in resp.text
    assert "Run scenario" in resp.text


@pytest.mark.skipif(not PYSPARK_AVAILABLE, reason="pyspark required")
def test_dataset_versions_endpoint_returns_json():
    rec = _ensure_sample_records()[0]
    client = TestClient(contracts_app)
    resp = client.get(f"/datasets/{rec.dataset_name}/{rec.dataset_version}.json")
    assert resp.status_code == 200
    payload = resp.json()
    assert payload["dataset_name"] == rec.dataset_name
    assert payload["dataset_version"] == rec.dataset_version


@pytest.mark.skipif(not PYSPARK_AVAILABLE, reason="pyspark required")
def test_pipeline_runs_json_endpoint(monkeypatch):
    rec = _ensure_sample_records()[0]

    async def fake_to_thread(func: Any, /, *args: Any, **kwargs: Any) -> Any:
        return func(*args, **kwargs)

    monkeypatch.setattr(asyncio, "to_thread", fake_to_thread)

    client = TestClient(demo_app)
    resp = client.get("/pipeline-runs.json")
    assert resp.status_code == 200
    payload = resp.json()
    assert isinstance(payload, list) and payload

    datasets = {item["dataset_name"] for item in payload}
    assert rec.dataset_name in datasets


@pytest.mark.skipif(not PYSPARK_AVAILABLE, reason="pyspark required")
def test_dataset_versions_endpoint_json_handles_missing():
    _ensure_sample_records()
    client = TestClient(contracts_app)
    resp = client.get("/datasets/missing.json")
    assert resp.status_code == 404
    data = resp.json()
    assert data["detail"] == "Dataset not found"


@pytest.mark.skipif(not PYSPARK_AVAILABLE, reason="pyspark required")
def test_dataset_versions_endpoint_returns_latest_metadata():
    rec = _ensure_sample_records()[0]
    client = TestClient(contracts_app)
    resp = client.get(f"/datasets/{rec.dataset_name}.json")
    assert resp.status_code == 200
    payload = resp.json()
    assert payload["dataset_name"] == rec.dataset_name
    assert payload["dataset_version"] == rec.dataset_version


@pytest.mark.skipif(not PYSPARK_AVAILABLE, reason="pyspark required")
def test_contract_versions_endpoint_json():
    rec = _ensure_sample_records()[0]
    client = TestClient(contracts_app)
    resp = client.get(f"/contracts/{rec.contract_id}.json")
    assert resp.status_code == 200
    payload = resp.json()
    assert rec.contract_version in payload["versions"]


@pytest.mark.skipif(not PYSPARK_AVAILABLE, reason="pyspark required")
def test_contract_endpoint_json():
    rec = _ensure_sample_records()[0]
    client = TestClient(contracts_app)
    resp = client.get(f"/contracts/{rec.contract_id}/{rec.contract_version}.json")
    assert resp.status_code == 200
    payload = resp.json()
    assert payload["contract_id"] == rec.contract_id
    assert payload["contract_version"] == rec.contract_version


@pytest.mark.skipif(not PYSPARK_AVAILABLE, reason="pyspark required")
def test_pipeline_runs_page_scenario_rows():
    records = _ensure_sample_records()
    rows = scenario_run_rows(records, SCENARIOS)
    assert rows
    keys = {row["key"] for row in rows}
    assert "ok" in keys


@pytest.mark.skipif(not PYSPARK_AVAILABLE, reason="pyspark required")
def test_dataset_versions_page_history_table():
    rec = _ensure_sample_records()[0]
    client = TestClient(contracts_app)
    resp = client.get(f"/datasets/{rec.dataset_name}")
    assert resp.status_code == 200
    assert rec.dataset_version in resp.text
    assert "table-sort-control" in resp.text


@pytest.mark.skipif(not PYSPARK_AVAILABLE, reason="pyspark required")
def test_contract_detail_versions_table():
    rec = _ensure_sample_records()[0]
    client = TestClient(contracts_app)
    resp = client.get(f"/contracts/{rec.contract_id}")
    assert resp.status_code == 200
    assert rec.contract_version in resp.text


@pytest.mark.skipif(not PYSPARK_AVAILABLE, reason="pyspark required")
def test_dataset_json_endpoint_contains_metrics():
    rec = _ensure_sample_records()[0]
    client = TestClient(contracts_app)
    resp = client.get(f"/datasets/{rec.dataset_name}/{rec.dataset_version}.json")
    assert resp.status_code == 200
    payload = resp.json()
    assert payload["dataset_name"] == rec.dataset_name
    assert payload["dataset_version"] == rec.dataset_version
    assert "dq_details" in payload


@pytest.mark.skipif(not PYSPARK_AVAILABLE, reason="pyspark required")
def test_pipeline_runs_page_filters_by_scenario():
    _ensure_sample_records()
    client = TestClient(demo_app)
    resp = client.get("/pipeline-runs?scenario=ok")
    assert resp.status_code == 200
    assert "orders" in resp.text


@pytest.mark.skipif(not PYSPARK_AVAILABLE, reason="pyspark required")
def test_pipeline_runs_page_filters_by_status():
    _ensure_sample_records()
    client = TestClient(demo_app)
    resp = client.get("/pipeline-runs?status=ok")
    assert resp.status_code == 200
    assert "orders" in resp.text


@pytest.mark.skipif(not PYSPARK_AVAILABLE, reason="pyspark required")
def test_pipeline_runs_page_json_endpoint_filters():
    _ensure_sample_records()
    client = TestClient(demo_app)
    resp = client.get("/pipeline-runs.json?scenario=ok")
    assert resp.status_code == 200
    payload = resp.json()
    assert payload
    assert all(item["scenario_key"] == "ok" for item in payload)


@pytest.mark.skipif(not PYSPARK_AVAILABLE, reason="pyspark required")
def test_datasets_json_endpoint_lists_datasets():
    _ensure_sample_records()
    client = TestClient(contracts_app)
    resp = client.get("/datasets.json")
    assert resp.status_code == 200
    payload = resp.json()
    assert payload
    assert "orders_enriched" in payload["datasets"]
