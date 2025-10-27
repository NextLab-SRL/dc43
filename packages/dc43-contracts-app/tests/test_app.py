import contextlib
import json
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from open_data_contract_standard.model import (
    Description,
    OpenDataContractStandard,
    SchemaObject,
    SchemaProperty,
)

from dc43_contracts_app import server


@pytest.fixture()
def client() -> TestClient:
    return TestClient(server.app)


def test_contracts_index(client: TestClient) -> None:
    resp = client.get("/contracts")
    assert resp.status_code == 200
    assert "Contracts" in resp.text


def test_datasets_index(client: TestClient) -> None:
    resp = client.get("/datasets")
    assert resp.status_code == 200
    assert "datasets" in resp.text.lower()


def test_summarise_metrics_groups_snapshots() -> None:
    summary = server._summarise_metrics(
        [
            {
                "dataset_id": "orders",
                "dataset_version": "2024-05-01",
                "contract_id": "orders",
                "contract_version": "1.0.0",
                "status_recorded_at": "2024-05-02T12:00:00Z",
                "metric_key": "row_count",
                "metric_value": 12,
                "metric_numeric_value": 12.0,
            },
            {
                "dataset_id": "orders",
                "dataset_version": "2024-05-01",
                "contract_id": "orders",
                "contract_version": "1.0.0",
                "status_recorded_at": "2024-05-02T12:00:00Z",
                "metric_key": "violations.total",
                "metric_value": 1,
                "metric_numeric_value": 1.0,
            },
            {
                "dataset_id": "orders",
                "dataset_version": "2024-04-30",
                "contract_id": "orders",
                "contract_version": "1.0.0",
                "status_recorded_at": "2024-05-01T08:00:00Z",
                "metric_key": "row_count",
                "metric_value": 10,
                "metric_numeric_value": 10.0,
            },
        ]
    )
    assert summary["metric_keys"] == ["row_count", "violations.total"]
    assert summary["numeric_metric_keys"] == ["row_count", "violations.total"]
    chronological = summary["chronological_history"]
    assert chronological[0]["dataset_version"] == "2024-04-30"
    assert chronological[-1]["dataset_version"] == "2024-05-01"
    latest = summary["latest"]
    assert latest is not None
    assert latest["dataset_version"] == "2024-05-01"
    assert any(metric["key"] == "violations.total" for metric in latest["metrics"])
    assert summary["previous"]


def test_dataset_detail_includes_metrics(monkeypatch, client: TestClient) -> None:
    dataset_path = server.DATASETS_FILE
    dataset_path.parent.mkdir(parents=True, exist_ok=True)
    original = dataset_path.read_text() if dataset_path.exists() else None

    record = {
        "contract_id": "demo_contract",
        "contract_version": "1.0.0",
        "dataset_name": "demo_dataset",
        "dataset_version": "2024-01-01",
        "status": "ok",
        "dq_details": {"output": {}},
        "run_type": "batch",
    }
    dataset_path.write_text(json.dumps([record]), encoding="utf-8")

    sample_metrics = [
        {
            "dataset_id": "demo_dataset",
            "dataset_version": "2024-01-01",
            "contract_id": "demo_contract",
            "contract_version": "1.0.0",
            "status_recorded_at": "2024-05-01T12:00:00Z",
            "metric_key": "row_count",
            "metric_value": 12,
            "metric_numeric_value": 12.0,
        },
        {
            "dataset_id": "demo_dataset",
            "dataset_version": "2024-01-01",
            "contract_id": "demo_contract",
            "contract_version": "1.0.0",
            "status_recorded_at": "2024-04-30T12:00:00Z",
            "metric_key": "violations.total",
            "metric_value": 1,
            "metric_numeric_value": 1.0,
        },
    ]
    calls: list[dict[str, object]] = []

    class DummyGovernanceClient:
        def get_metrics(self, **kwargs):
            calls.append(kwargs)
            return sample_metrics

    def fake_thread_clients():
        return (object(), object(), DummyGovernanceClient())

    monkeypatch.setattr(server, "_thread_service_clients", fake_thread_clients)

    try:
        resp = client.get("/datasets/demo_dataset/2024-01-01")
    finally:
        if original is None:
            dataset_path.unlink(missing_ok=True)
        else:
            dataset_path.write_text(original, encoding="utf-8")

    assert resp.status_code == 200
    body = resp.text
    assert "<code>row_count</code>" in body
    assert "Earlier metric snapshots" in body
    assert 'id="dataset-metric-trends"' in body
    assert calls
    assert calls[0]["dataset_id"] == "demo_dataset"
    assert calls[0]["dataset_version"] == "2024-01-01"


def test_contract_detail_includes_metric_chart(monkeypatch, client: TestClient) -> None:
    contract_id = "demo_contract"
    contract_version = "1.0.0"
    contract_model = OpenDataContractStandard(
        version=contract_version,
        kind="DataContract",
        apiVersion="3.0.2",
        id=contract_id,
        name="Demo Contract",
        description=Description(usage="Demo"),
        schema=[
            SchemaObject(
                name="demo",
                properties=[
                    SchemaProperty(name="id", physicalType="string", required=True),
                ],
            )
        ],
    )
    contract_dir = Path(server.CONTRACT_DIR)
    contract_file = contract_dir / contract_id / f"{contract_version}.json"
    contract_file.parent.mkdir(parents=True, exist_ok=True)
    original_contract = contract_file.read_text(encoding="utf-8") if contract_file.exists() else None
    server.store.put(contract_model)

    sample_metrics = [
        {
            "dataset_id": contract_id,
            "dataset_version": "2024-01-01",
            "contract_id": contract_id,
            "contract_version": contract_version,
            "status_recorded_at": "2024-05-01T12:00:00Z",
            "metric_key": "row_count",
            "metric_value": 12,
            "metric_numeric_value": 12.0,
        },
        {
            "dataset_id": contract_id,
            "dataset_version": "2024-04-30",
            "contract_id": contract_id,
            "contract_version": contract_version,
            "status_recorded_at": "2024-04-30T12:00:00Z",
            "metric_key": "violations.total",
            "metric_value": 1,
            "metric_numeric_value": 1.0,
        },
    ]

    calls: list[dict[str, object]] = []

    class DummyGovernanceClient:
        def get_metrics(self, **kwargs):
            calls.append(kwargs)
            return sample_metrics

    def fake_thread_clients():
        return (object(), object(), DummyGovernanceClient())

    monkeypatch.setattr(server, "_thread_service_clients", fake_thread_clients)

    try:
        resp = client.get(f"/contracts/{contract_id}/{contract_version}")
    finally:
        if original_contract is None:
            contract_file.unlink(missing_ok=True)
            with contextlib.suppress(OSError):
                contract_file.parent.rmdir()
        else:
            contract_file.write_text(original_contract, encoding="utf-8")

    assert resp.status_code == 200
    body = resp.text
    assert 'id="contract-metric-trends"' in body
    assert calls
    first = calls[0]
    assert first["contract_id"] == contract_id
    assert first["contract_version"] == contract_version
    assert first["dataset_id"] == contract_id
