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


def test_summarise_metrics_coerces_numeric_values() -> None:
    summary = server._summarise_metrics(
        [
            {
                "dataset_id": "orders",
                "dataset_version": "2024-06-01",
                "contract_id": "orders",
                "contract_version": "1.0.0",
                "status_recorded_at": "2024-06-02T00:00:00Z",
                "metric_key": "row_count",
                "metric_value": "15",
            }
        ]
    )
    assert summary["numeric_metric_keys"] == ["row_count"]
    latest = summary["latest"]
    assert latest is not None
    row_count = next(metric for metric in latest["metrics"] if metric["key"] == "row_count")
    assert row_count["numeric_value"] == 15.0


def test_summarise_metrics_includes_contract_filters() -> None:
    summary = server._summarise_metrics(
        [
            {
                "dataset_id": "orders",
                "dataset_version": "2024-06-01",
                "contract_id": "orders",
                "contract_version": "1.0.0",
                "status_recorded_at": "2024-06-02T00:00:00Z",
                "metric_key": "row_count",
                "metric_value": 10,
                "metric_numeric_value": 10,
            },
            {
                "dataset_id": "orders",
                "dataset_version": "2024-06-02",
                "contract_id": "orders",
                "contract_version": "2.0.0",
                "status_recorded_at": "2024-06-03T00:00:00Z",
                "metric_key": "row_count",
                "metric_value": 12,
                "metric_numeric_value": 12,
            },
        ]
    )
    assert summary["contract_filters"] == [
        {
            "contract_id": "orders",
            "label": "orders",
            "versions": ["1.0.0", "2.0.0"],
            "version_source": "contract",
        }
    ]


def test_summarise_metrics_decodes_json_strings() -> None:
    summary = server._summarise_metrics(
        [
            {
                "dataset_id": "orders",
                "dataset_version": "2024-06-01",
                "contract_id": "orders",
                "contract_version": "1.0.0",
                "status_recorded_at": "2024-06-02T00:00:00Z",
                "metric_key": "row_count",
                "metric_value": '"15"',
            },
            {
                "dataset_id": "orders",
                "dataset_version": "2024-06-01",
                "contract_id": "orders",
                "contract_version": "1.0.0",
                "status_recorded_at": "2024-06-02T00:00:00Z",
                "metric_key": "payload",
                "metric_value": '{"failures": 2}',
            },
        ]
    )
    assert summary["numeric_metric_keys"] == ["row_count"]
    latest = summary["latest"]
    assert latest is not None
    metrics = {metric["key"]: metric for metric in latest["metrics"]}
    assert metrics["row_count"]["numeric_value"] == 15.0
    assert metrics["row_count"]["value"] == "15"
    assert metrics["payload"]["value"] == "{\"failures\": 2}"
    assert metrics["payload"]["raw_value"] == {"failures": 2}


def test_summarise_metrics_falls_back_to_dataset_versions_for_filters() -> None:
    summary = server._summarise_metrics(
        [
            {
                "dataset_id": "orders",
                "dataset_version": "2024-06-01",
                "contract_id": "orders",
                "contract_version": "",
                "status_recorded_at": "2024-06-02T00:00:00Z",
                "metric_key": "row_count",
                "metric_value": 10,
                "metric_numeric_value": 10,
            },
            {
                "dataset_id": "orders",
                "dataset_version": "2024-06-02",
                "contract_id": "orders",
                "contract_version": "",
                "status_recorded_at": "2024-06-03T00:00:00Z",
                "metric_key": "row_count",
                "metric_value": 12,
                "metric_numeric_value": 12,
            },
        ]
    )
    assert summary["contract_filters"] == [
        {
            "contract_id": "orders",
            "label": "orders",
            "versions": ["2024-06-01", "2024-06-02"],
            "version_source": "dataset",
        }
    ]


def test_contract_filter_fallbacks_extract_versions() -> None:
    fallbacks = server._contract_filter_fallbacks(
        [
            {
                "contract_id": "orders",
                "contract_version": "1.0.0",
                "dataset_version": "2024-06-01",
            },
            {
                "contract_id": "orders",
                "contract_version": "2.0.0",
                "dataset_version": "2024-06-02",
            },
            {
                "contract_id": "customers",
                "contract_version": "",
                "dataset_version": "2024-06-03",
            },
        ]
    )
    assert fallbacks["orders"]["contract_versions"] == {"1.0.0", "2.0.0"}
    assert fallbacks["orders"]["dataset_versions"] == {"2024-06-01", "2024-06-02"}
    assert fallbacks["customers"]["contract_versions"] == set()
    assert fallbacks["customers"]["dataset_versions"] == {"2024-06-03"}


def test_summarise_metrics_uses_fallback_contract_versions() -> None:
    fallbacks = {
        "orders": {
            "contract_versions": {"1.0.0"},
            "dataset_versions": {"2024-06-01"},
        }
    }
    summary = server._summarise_metrics(
        [
            {
                "dataset_id": "orders",
                "dataset_version": "",
                "contract_id": "orders",
                "contract_version": "",
                "status_recorded_at": "2024-06-02T00:00:00Z",
                "metric_key": "row_count",
                "metric_value": 10,
                "metric_numeric_value": 10,
            }
        ],
        fallback_contract_versions=fallbacks,
    )
    assert summary["contract_filters"] == [
        {
            "contract_id": "orders",
            "label": "orders",
            "versions": ["1.0.0"],
            "version_source": "contract",
        }
    ]


def test_summarise_metrics_uses_dataset_version_fallbacks() -> None:
    fallbacks = {
        "orders": {
            "contract_versions": set(),
            "dataset_versions": {"2024-06-01", "2024-06-02"},
        }
    }
    summary = server._summarise_metrics(
        [
            {
                "dataset_id": "orders",
                "dataset_version": "",
                "contract_id": "orders",
                "contract_version": "",
                "status_recorded_at": "2024-06-02T00:00:00Z",
                "metric_key": "row_count",
                "metric_value": 10,
                "metric_numeric_value": 10,
            }
        ],
        fallback_contract_versions=fallbacks,
    )
    assert summary["contract_filters"] == [
        {
            "contract_id": "orders",
            "label": "orders",
            "versions": ["2024-06-01", "2024-06-02"],
            "version_source": "dataset",
        }
    ]

def test_dataset_detail_returns_not_found(client: TestClient) -> None:
    resp = client.get("/datasets/demo_dataset/2024-01-01")
    assert resp.status_code == 404


def test_dataset_detail_displays_observation_scope(
    monkeypatch: pytest.MonkeyPatch,
    client: TestClient,
) -> None:
    record = server.DatasetRecord(
        contract_id="",
        contract_version="",
        dataset_name="demo.dataset",
        dataset_version="2024-01-01",
        status="ok",
    )
    record.observation_label = "Pre-write dataframe snapshot"

    monkeypatch.setattr(server, "load_records", lambda **_: [record])
    monkeypatch.setattr(server, "data_products_for_dataset", lambda *_: [])
    monkeypatch.setattr(server, "_dataset_preview", lambda *_, **__: None)

    resp = client.get("/datasets/demo.dataset/2024-01-01")

    assert resp.status_code == 200
    assert "Pre-write dataframe snapshot" in resp.text


def test_dataset_detail_falls_back_to_dataset_metrics(
    monkeypatch: pytest.MonkeyPatch,
    client: TestClient,
) -> None:
    record = server.DatasetRecord(
        contract_id="demo.contract",
        contract_version="1.0.0",
        dataset_name="demo.dataset",
        dataset_version="2024-01-01T12:00:00Z",
        status="ok",
    )

    monkeypatch.setattr(server, "load_records", lambda **_: [record])
    monkeypatch.setattr(server, "data_products_for_dataset", lambda *_: [])
    monkeypatch.setattr(server, "_dataset_preview", lambda *_, **__: None)

    calls: list[dict[str, object]] = []

    class DummyGovernanceClient:
        def __init__(self) -> None:
            self._responses = [[], [
                {
                    "dataset_id": record.dataset_name,
                    "dataset_version": record.dataset_version,
                    "contract_id": record.contract_id,
                    "contract_version": record.contract_version,
                    "status_recorded_at": "2024-01-02T00:00:00Z",
                    "metric_key": "row_count",
                    "metric_value": 10,
                    "metric_numeric_value": 10.0,
                }
            ]]

        def get_metrics(self, **kwargs):
            calls.append(kwargs)
            return self._responses.pop(0)

    dummy_client = DummyGovernanceClient()

    def fake_thread_clients():
        return (object(), object(), dummy_client)

    monkeypatch.setattr(server, "_thread_service_clients", fake_thread_clients)

    resp = client.get("/datasets/demo.dataset/2024-01-01T12:00:00Z")

    assert resp.status_code == 200
    assert "row_count" in resp.text
    assert len(calls) == 2
    first, second = calls
    assert first.get("contract_id") == "demo.contract"
    assert "contract_id" not in second


def test_dataset_detail_limits_record_fetch(
    monkeypatch: pytest.MonkeyPatch,
    client: TestClient,
) -> None:
    record = server.DatasetRecord(
        contract_id="demo.contract",
        contract_version="1.0.0",
        dataset_name="demo.dataset",
        dataset_version="2024-01-01",
        status="ok",
    )

    captured_kwargs: dict[str, object] = {}

    def fake_load_records(**kwargs):
        captured_kwargs.update(kwargs)
        return [record]

    monkeypatch.setattr(server, "load_records", fake_load_records)
    monkeypatch.setattr(server, "data_products_for_dataset", lambda *_: [])
    monkeypatch.setattr(server, "_dataset_preview", lambda *_, **__: None)

    resp = client.get("/datasets/demo.dataset/2024-01-01")

    assert resp.status_code == 200
    assert captured_kwargs["dataset_id"] == "demo.dataset"
    assert "dataset_version" not in captured_kwargs


def test_metric_chart_bundle_served(client: TestClient) -> None:
    resp = client.get("/static/chart.umd.js")
    assert resp.status_code == 200
    assert b"Chart" in resp.content


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


def test_dataset_versions_page_renders_chart_without_numeric_metrics(
    monkeypatch: pytest.MonkeyPatch,
    client: TestClient,
) -> None:
    record = server.DatasetRecord(
        contract_id="demo.contract",
        contract_version="1.0.0",
        dataset_name="demo.dataset",
        dataset_version="2024-01-01T00:00:00Z",
        status="ok",
    )

    monkeypatch.setattr(server, "load_records", lambda **_: [record])

    class DummyGovernanceClient:
        def get_metrics(self, **kwargs):  # noqa: D401 - simple stub
            assert kwargs["dataset_id"] == record.dataset_name
            return [
                {
                    "dataset_id": record.dataset_name,
                    "dataset_version": record.dataset_version,
                    "contract_id": record.contract_id,
                    "contract_version": record.contract_version,
                    "status_recorded_at": "2024-01-02T00:00:00Z",
                    "metric_key": "dq_state",
                    "metric_value": "passed",
                    "metric_numeric_value": None,
                }
            ]

    monkeypatch.setattr(
        server,
        "_thread_service_clients",
        lambda: (object(), object(), DummyGovernanceClient()),
    )

    resp = client.get(f"/datasets/{record.dataset_name}")

    assert resp.status_code == 200
    assert 'id="dataset-metric-trends"' in resp.text
    assert "Metric trend data will appear once the chart loads." in resp.text


def test_load_records_normalises_backend_status(monkeypatch: pytest.MonkeyPatch) -> None:
    details = {
        "metrics": {"violations.total": 5},
        "failed_expectations": {"schema": {"count": 3}},
        "errors": ["schema-mismatch", "extra-column"],
        "dq_status": {"violations": 6},
    }

    monkeypatch.setattr(server, "list_dataset_ids", lambda: ["sales.orders"])

    def fake_activity(
        dataset_id: str,
        dataset_version: str | None = None,
        *,
        include_status: bool = False,
    ) -> list[dict[str, object]]:
        assert dataset_id == "sales.orders"
        assert dataset_version is None
        assert include_status is True
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
                "validation_status": {
                    "status": "ok",
                    "details": details,
                    "reason": "All good",
                },
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

    assert not calls


def test_load_records_uses_event_status_when_backend_missing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(server, "list_dataset_ids", lambda: ["inventory.stock"])

    def fake_activity(
        _: str,
        dataset_version: str | None = None,
        *,
        include_status: bool = False,
    ) -> list[dict[str, object]]:
        assert include_status is True
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


def test_load_records_deduplicates_dataset_versions(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(server, "list_dataset_ids", lambda: ["sales.orders"])

    first_recorded_at = "2025-11-14T20:04:16.238229Z"
    second_recorded_at = "2025-11-14T20:05:00.000000Z"

    def fake_activity(
        _: str,
        dataset_version: str | None = None,
        *,
        include_status: bool = False,
    ) -> list[dict[str, object]]:
        assert include_status is True
        return [
            {
                "dataset_version": "2025-11-14T20:04:16.238229Z",
                "contract_id": "sales.contract",
                "contract_version": "0.2.0",
                "events": [
                    {
                        "dq_status": "warn",
                        "dq_details": {"recorded_at": first_recorded_at},
                        "recorded_at": first_recorded_at,
                    }
                ],
            },
            {
                "dataset_version": "2025-11-14T20:04:16.238229Z",
                "contract_id": "sales.contract",
                "contract_version": "0.2.0",
                "events": [
                    {
                        "dq_status": "ok",
                        "dq_details": {"recorded_at": second_recorded_at},
                        "recorded_at": second_recorded_at,
                    }
                ],
            },
        ]

    monkeypatch.setattr(server, "dataset_pipeline_activity", fake_activity)
    monkeypatch.setattr(server, "dataset_validation_status", lambda **_: None)

    records = server.load_records()

    assert len(records) == 1
    record = records[0]

    assert record.dataset_version == "2025-11-14T20:04:16.238229Z"
    assert record.contract_version == "0.2.0"
    assert record.status == "ok"


def test_dataset_history_sorting_orders_by_version_then_contract() -> None:
    rows = [
        {"dataset_version": "2024-01-01T00:00:00Z", "contract_id": "b", "contract_version": "1.0.0"},
        {"dataset_version": "2024-01-01T00:00:00Z", "contract_id": "a", "contract_version": "2.0.0"},
        {"dataset_version": "2024-02-01T00:00:00Z", "contract_id": "a", "contract_version": "0.1.0"},
    ]

    sorted_rows = server._sort_dataset_history_rows(rows)

    assert [row["dataset_version"] for row in sorted_rows] == [
        "2024-02-01T00:00:00Z",
        "2024-01-01T00:00:00Z",
        "2024-01-01T00:00:00Z",
    ]
    assert [row["contract_id"] for row in sorted_rows[1:]] == ["b", "a"]


def test_dataset_catalog_prefers_dataset_names_from_records(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    dataset_name = "dev.catalog.schema.table_ds"
    contract_id = "dev.catalog.schema.table"
    contract_version = "0.3.0"
    record = server.DatasetRecord(
        contract_id=contract_id,
        contract_version=contract_version,
        dataset_name=dataset_name,
        dataset_version="2025-11-14T20:04:41.633019Z",
        status="ok",
    )

    monkeypatch.setattr(server, "list_contract_ids", lambda: [contract_id])
    monkeypatch.setattr(server, "contract_versions", lambda cid: [contract_version])

    def fake_get_contract(cid: str, ver: str) -> SimpleNamespace:
        assert cid == contract_id
        assert ver == contract_version
        return SimpleNamespace(id=cid, version=ver, status="active", servers=[])

    monkeypatch.setattr(server, "get_contract", fake_get_contract)
    monkeypatch.setattr(server, "_server_details", lambda _: {})
    monkeypatch.setattr(server, "data_products_for_dataset", lambda *_: [])

    catalog = server.dataset_catalog([record])

    assert len(catalog) == 1
    entry = catalog[0]
    assert entry["dataset_name"] == dataset_name
    assert entry["contract_summaries"]
    assert any(item["id"] == contract_id for item in entry["contract_summaries"])
    assert all(item["dataset_name"] != contract_id for item in catalog)


def test_dataset_catalog_falls_back_to_contract_id_when_unknown_dataset(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    contract_id = "dev.catalog.schema.table"

    monkeypatch.setattr(server, "list_contract_ids", lambda: [contract_id])
    monkeypatch.setattr(server, "contract_versions", lambda cid: ["0.1.0"])
    monkeypatch.setattr(
        server,
        "get_contract",
        lambda cid, ver: SimpleNamespace(id=cid, version=ver, status="active", servers=[]),
    )
    monkeypatch.setattr(server, "_server_details", lambda _: {})
    monkeypatch.setattr(server, "data_products_for_dataset", lambda *_: [])

    catalog = server.dataset_catalog([])

    assert len(catalog) == 1
    entry = catalog[0]
    assert entry["dataset_name"] == contract_id
    assert entry["contract_summaries"]
    assert entry["contract_summaries"][0]["id"] == contract_id


def test_dataset_catalog_handles_records_missing_scope_metadata(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    record = server.DatasetRecord(
        contract_id="demo.contract",
        contract_version="1.0.0",
        dataset_name="demo.contract_ds",
        dataset_version="2025-11-17T05:08:00Z",
        status="ok",
    )

    delattr(record, "observation_label")
    delattr(record, "observation_scope")
    delattr(record, "observation_operation")

    monkeypatch.setattr(server, "list_contract_ids", lambda: [])
    monkeypatch.setattr(server, "data_products_for_dataset", lambda *_: [])

    catalog = server.dataset_catalog([record])

    assert len(catalog) == 1
    entry = catalog[0]
    assert entry["dataset_name"] == "demo.contract_ds"
    assert entry["latest_observation_label"] == ""
    assert entry["latest_observation_scope"] == ""
    assert entry["latest_observation_operation"] == ""


def test_next_version_handles_draft_suffix() -> None:
    assert server._next_version("0.2.0-draft") == "0.2.1"


def test_next_version_passthrough_for_unparseable_values() -> None:
    assert server._next_version("snapshot-20240512") == "snapshot-20240512"
