import shutil
import sys
import types
from pathlib import Path
from typing import Any, Dict, List

import pytest
from fastapi.testclient import TestClient

from dc43_demo_app import contracts_api
from dc43_demo_app.contracts_workspace import current_workspace
from dc43_demo_app.server import app


@pytest.fixture
def fake_dlt_module(monkeypatch: pytest.MonkeyPatch) -> List[Dict[str, Any]]:
    runs: List[Dict[str, Any]] = []

    class FakeHarness:
        def __init__(self, pipeline_callable, **_kwargs: Any) -> None:
            self._pipeline = pipeline_callable

        def __enter__(self) -> "FakeHarness":
            return self

        def __exit__(self, exc_type, exc, tb) -> bool:
            return False

        def run(self, **run_kwargs: Any) -> Dict[str, Any]:
            runs.append(dict(run_kwargs))
            return self._pipeline(**run_kwargs)

    def build_pipeline(**_kwargs: Any):
        def _pipeline(**kwargs: Any) -> Dict[str, Any]:
            dataset = kwargs.get("dataset_name") or "orders_enriched"
            version = kwargs.get("dataset_version") or "dlt-version"
            status = kwargs.get("status") or "ok"
            return {
                "dataset": dataset,
                "dataset_version": version,
                "output": {
                    "dq_status": {"status": status},
                    "metrics": {"violations.total": 0},
                },
                "status": status,
            }

        return _pipeline

    module = types.SimpleNamespace(
        LocalDLTHarness=FakeHarness,
        build_pipeline=build_pipeline,
    )

    monkeypatch.setitem(
        sys.modules,
        "dc43_integrations.spark.dlt.contract_table",
        module,
    )
    return runs


def _active_version(dataset: str) -> str | None:
    workspace = current_workspace()
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


def test_dlt_pipeline_records_run(fake_dlt_module: List[Dict[str, Any]]) -> None:
    from dc43_demo_app.dlt_pipeline import run_dlt_pipeline

    original_records = contracts_api.load_records()
    dataset = "orders_enriched"
    previous_version = _active_version(dataset)
    dataset_version: str | None = None

    try:
        dataset_name, dataset_version = run_dlt_pipeline(
            contract_id="orders_enriched",
            contract_version="1.1.0",
            dataset_name=None,
            dataset_version=None,
            run_type="enforce",
            scenario_key="dlt-contract-table",
            dlt_config={"run": {"full_refresh": True}},
        )

        assert dataset_name == "orders_enriched"
        assert fake_dlt_module, "DLT harness should be invoked"
        run_kwargs = fake_dlt_module[-1]
        assert run_kwargs["contract_id"] == "orders_enriched"
        assert run_kwargs["pipeline_context"]["pipeline"] == "dc43_demo_app.dlt_pipeline.run_dlt_pipeline"
        assert run_kwargs["pipeline_context"]["scenario_key"] == "dlt-contract-table"
        assert run_kwargs["full_refresh"] is True

        records = contracts_api.load_records()
        last = records[-1]
        assert last.dataset_name == dataset_name
        assert last.dataset_version == dataset_version
        assert last.scenario_key == "dlt-contract-table"
        assert last.status == "ok"
        output = last.dq_details.get("output", {})
        assert output.get("dq_status", {}).get("status") == "ok"
    finally:
        contracts_api.save_records(original_records)
        if previous_version:
            try:
                contracts_api.set_active_version(dataset, previous_version)
            except FileNotFoundError:
                pass
        if dataset_version:
            out_dir = Path(contracts_api.DATA_DIR) / dataset / dataset_version
            if out_dir.exists():
                shutil.rmtree(out_dir, ignore_errors=True)


def test_run_pipeline_endpoint_dispatches_dlt(fake_dlt_module: List[Dict[str, Any]]) -> None:
    client = TestClient(app)

    original_records = contracts_api.load_records()
    dataset = "orders_enriched"
    previous_enriched = _active_version(dataset)
    previous_orders = _active_version("orders")
    previous_customers = _active_version("customers")

    try:
        response = client.post(
            "/pipeline/run",
            data={"scenario": "dlt-contract-table"},
            follow_redirects=False,
        )
        assert response.status_code == 303
        assert response.headers["location"].startswith("/pipeline-runs")
        assert fake_dlt_module, "DLT harness should be invoked by the endpoint"
        run_kwargs = fake_dlt_module[-1]
        assert run_kwargs.get("full_refresh") is False
        assert run_kwargs.get("pipeline_context", {}).get("scenario_key") == "dlt-contract-table"

        records = contracts_api.load_records()
        last = records[-1]
        assert last.scenario_key == "dlt-contract-table"
        assert last.status == "ok"
        assert last.dataset_name == dataset
    finally:
        contracts_api.save_records(original_records)
        for name, version in (
            (dataset, previous_enriched),
            ("orders", previous_orders),
            ("customers", previous_customers),
        ):
            if version:
                try:
                    contracts_api.set_active_version(name, version)
                except FileNotFoundError:
                    pass
        produced_version = None
        if fake_dlt_module:
            produced_version = fake_dlt_module[-1].get("dataset_version")
        if produced_version:
            out_dir = Path(contracts_api.DATA_DIR) / dataset / str(produced_version)
            if out_dir.exists():
                shutil.rmtree(out_dir, ignore_errors=True)
