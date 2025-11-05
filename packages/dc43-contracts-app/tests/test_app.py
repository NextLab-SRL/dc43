from types import SimpleNamespace

import pytest
from fastapi.testclient import TestClient

from open_data_contract_standard.model import (
    Description,
    OpenDataContractStandard,
    SchemaObject,
    SchemaProperty,
)

from dc43_contracts_app import server
from dc43_contracts_app.services import store as contract_store


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


def test_dataset_detail_returns_not_found(client: TestClient) -> None:
    resp = client.get("/datasets/demo_dataset/2024-01-01")
    assert resp.status_code == 404


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
    contract_store.put(contract_model)

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

    resp = client.get(f"/contracts/{contract_id}/{contract_version}")

    assert resp.status_code == 200
    body = resp.text
    assert 'id="contract-metric-trends"' in body
    assert calls
    first = calls[0]
    assert first["contract_id"] == contract_id
    assert first["contract_version"] == contract_version
    assert first["dataset_id"] == contract_id


def test_load_records_normalises_backend_status(monkeypatch: pytest.MonkeyPatch) -> None:
    details = {
        "metrics": {"violations.total": 5},
        "failed_expectations": {"schema": {"count": 3}},
        "errors": ["schema-mismatch", "extra-column"],
        "dq_status": {"violations": 6},
    }

    monkeypatch.setattr(server, "list_dataset_ids", lambda: ["sales.orders"])

    def fake_activity(dataset_id: str) -> list[dict[str, object]]:
        assert dataset_id == "sales.orders"
        return [
            {
                "dataset_version": "2024-05-02",
                "contract_id": "sales.contract",
                "contract_version": "1.2.3",
                "events": [
                    {"dq_status": "warn"},
                    {
                        "dq_status": "ok",
                        "pipeline_context": {
                            "run_type": "scheduled",
                            "scenario_key": "scenario-123",
                        },
                        "data_product": {
                            "id": "product-x",
                            "port": "output",
                            "role": "publisher",
                        },
                    },
                ],
            }
        ]

    monkeypatch.setattr(server, "dataset_pipeline_activity", fake_activity)

    calls: list[dict[str, object]] = []

    def fake_validation_status(**kwargs):
        calls.append(kwargs)
        return SimpleNamespace(status="VALID", details=details, reason="All good")

    monkeypatch.setattr(server, "dataset_validation_status", fake_validation_status)

    records = server.load_records()

    assert len(records) == 1
    record = records[0]

    assert record.status == "ok"
    assert record.violations == 6
    assert record.dq_details == details
    assert record.run_type == "scheduled"
    assert record.scenario_key == "scenario-123"
    assert record.data_product_id == "product-x"
    assert record.data_product_port == "output"
    assert record.data_product_role == "publisher"
    assert record.reason == "All good"

    assert calls
    first_call = calls[0]
    assert first_call["contract_id"] == "sales.contract"
    assert first_call["contract_version"] == "1.2.3"
    assert first_call["dataset_id"] == "sales.orders"
    assert first_call["dataset_version"] == "2024-05-02"


def test_load_records_uses_event_status_when_backend_missing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(server, "list_dataset_ids", lambda: ["inventory.stock"])

    def fake_activity(_: str) -> list[dict[str, object]]:
        return [
            {
                "dataset_version": "2024-05-03",
                "contract_id": "inventory.contract",
                "contract_version": "2.0.0",
                "events": [
                    {
                        "dq_status": "ko",
                        "dq_details": {"errors": ["missing primary key", "null value"]},
                        "dq_reason": "Schema mismatch",
                        "pipeline_context": {"run_type": "adhoc"},
                        "scenario_key": "manual-run",
                    }
                ],
            }
        ]

    monkeypatch.setattr(server, "dataset_pipeline_activity", fake_activity)

    calls: list[dict[str, object]] = []

    def fake_validation_status(**kwargs):
        calls.append(kwargs)
        return None

    monkeypatch.setattr(server, "dataset_validation_status", fake_validation_status)

    records = server.load_records()

    assert len(records) == 1
    record = records[0]

    assert record.status == "block"
    assert record.violations == 2
    assert record.dq_details == {"errors": ["missing primary key", "null value"]}
    assert record.run_type == "adhoc"
    assert record.scenario_key == "manual-run"
    assert record.reason == "Schema mismatch"

    assert calls
    call = calls[0]
    assert call["contract_id"] == "inventory.contract"
    assert call["contract_version"] == "2.0.0"
    assert call["dataset_id"] == "inventory.stock"
    assert call["dataset_version"] == "2024-05-03"


def test_next_version_handles_draft_suffix() -> None:
    assert server._next_version("0.2.0-draft") == "0.2.1"


def test_next_version_passthrough_for_unparseable_values() -> None:
    assert server._next_version("snapshot-20240512") == "snapshot-20240512"
