from __future__ import annotations

import json
from pathlib import Path

import pytest

pytest.importorskip("sqlalchemy")

from dc43_service_clients.data_quality import ValidationResult
from dc43_service_backends.governance.storage.sql import SQLGovernanceStore


@pytest.fixture()
def sql_engine(tmp_path: Path):
    from sqlalchemy import create_engine

    db_path = tmp_path / "governance.db"
    engine = create_engine(f"sqlite:///{db_path}")
    try:
        yield engine
    finally:
        engine.dispose()


def test_load_pipeline_activity_injects_dataset_version(sql_engine) -> None:
    store = SQLGovernanceStore(sql_engine)

    store._write_payload(  # type: ignore[attr-defined]
        store._activity,  # type: ignore[attr-defined]
        dataset_id="sales.orders",
        dataset_version="0.1.0",
        payload={
            "contract_id": "sales.orders",
            "contract_version": "0.1.0",
            "events": [
                {
                    "operation": "write",
                    "recorded_at": "2025-10-28T09:13:12.628161Z",
                }
            ],
        },
        extra={"updated_at": "2025-10-28T09:13:12.628161Z"},
    )

    activities = store.load_pipeline_activity(dataset_id="sales.orders")
    assert len(activities) == 1
    record = activities[0]
    assert record["dataset_id"] == "sales.orders"
    assert record["dataset_version"] == "0.1.0"
    assert record["contract_id"] == "sales.orders"
    assert record["contract_version"] == "0.1.0"
    assert record["events"] == [
        {
            "operation": "write",
            "recorded_at": "2025-10-28T09:13:12.628161Z",
        }
    ]

    single = store.load_pipeline_activity(
        dataset_id="sales.orders", dataset_version="0.1.0"
    )
    assert single == activities


def test_load_status_handles_duplicate_rows(sql_engine) -> None:
    payload_old = {
        "contract_id": "sales.orders",
        "contract_version": "0.1.0",
        "dataset_id": "sales.orders",
        "dataset_version": "0.1.0",
        "status": "warn",
        "reason": "initial run",
        "details": {"checks": ["row-count"]},
    }
    payload_new = dict(payload_old, status="ok", reason="latest run")

    with sql_engine.begin() as conn:
        conn.exec_driver_sql(
            """
            CREATE TABLE dq_status (
                dataset_id TEXT,
                dataset_version TEXT,
                contract_id TEXT,
                contract_version TEXT,
                payload TEXT,
                recorded_at TEXT
            )
            """
        )
        conn.exec_driver_sql(
            """
            INSERT INTO dq_status (
                dataset_id,
                dataset_version,
                contract_id,
                contract_version,
                payload,
                recorded_at
            ) VALUES (?, ?, ?, ?, ?, ?)
            """,
            (
                "sales.orders",
                "0.1.0",
                "sales.orders",
                "0.1.0",
                json.dumps(payload_old),
                "2024-01-01T00:00:00Z",
            ),
        )
        conn.exec_driver_sql(
            """
            INSERT INTO dq_status (
                dataset_id,
                dataset_version,
                contract_id,
                contract_version,
                payload,
                recorded_at
            ) VALUES (?, ?, ?, ?, ?, ?)
            """,
            (
                "sales.orders",
                "0.1.0",
                "sales.orders",
                "0.1.0",
                json.dumps(payload_new),
                "2024-01-02T00:00:00Z",
            ),
        )

    store = SQLGovernanceStore(sql_engine)

    result = store.load_status(
        contract_id="sales.orders",
        contract_version="0.1.0",
        dataset_id="sales.orders",
        dataset_version="0.1.0",
    )

    assert result is not None
    assert result.status == "ok"
    assert result.reason == "latest run"


def test_sql_store_extracts_metrics_from_details(sql_engine) -> None:
    store = SQLGovernanceStore(sql_engine)

    status = ValidationResult(status="ok")
    status.details = {"metrics": {"row_count": 3}}

    store.save_status(
        contract_id="sales.orders",
        contract_version="1.0.0",
        dataset_id="sales.orders",
        dataset_version="2024-06-01",
        status=status,
    )

    with sql_engine.begin() as conn:
        rows = conn.exec_driver_sql(
            "SELECT metric_key, metric_numeric_value FROM dq_metrics"
        ).fetchall()

    assert rows == [("row_count", 3.0)]


def test_sql_store_normalises_string_metrics(sql_engine) -> None:
    store = SQLGovernanceStore(sql_engine)

    status = ValidationResult(status="ok", metrics={"row_count": "12", "notes": "pass"})

    store.save_status(
        contract_id="sales.orders",
        contract_version="1.0.0",
        dataset_id="sales.orders",
        dataset_version="2024-06-01",
        status=status,
    )

    records = store.load_metrics(dataset_id="sales.orders")
    assert any(
        row["metric_key"] == "row_count" and row["metric_numeric_value"] == 12.0
        for row in records
    )
    assert any(
        row["metric_key"] == "notes" and row["metric_value"] == "pass"
        for row in records
    )


def test_sql_store_respects_explicit_metrics_table(sql_engine) -> None:
    store = SQLGovernanceStore(sql_engine, metrics_table="dq_status_metrics")

    assert store._metrics.name == "dq_status_metrics"  # type: ignore[attr-defined]


def test_sql_store_derives_metrics_table_from_status(sql_engine) -> None:
    store = SQLGovernanceStore(sql_engine, status_table="orders_dq_status")

    assert store._metrics.name == "orders_dq_metrics"  # type: ignore[attr-defined]


def test_sql_store_defaults_to_legacy_metrics_table(sql_engine) -> None:
    store = SQLGovernanceStore(sql_engine)

    assert store._metrics.name == "dq_metrics"  # type: ignore[attr-defined]


def test_sql_store_load_status_matrix_entries_filters(sql_engine) -> None:
    store = SQLGovernanceStore(sql_engine)

    store.save_status(
        contract_id="sales.orders",
        contract_version="1.0.0",
        dataset_id="sales.orders",
        dataset_version="2024-01-01",
        status=ValidationResult(status="ok"),
    )
    store.save_status(
        contract_id="sales.orders",
        contract_version="1.1.0",
        dataset_id="sales.orders",
        dataset_version="2024-01-02",
        status=ValidationResult(status="warn"),
    )

    entries = store.load_status_matrix_entries(
        dataset_id="sales.orders",
        dataset_versions=["2024-01-02"],
        contract_ids=["sales.orders"],
    )

    assert len(entries) == 1
    entry = entries[0]
    assert entry["dataset_version"] == "2024-01-02"
    status = entry["status"]
    assert isinstance(status, ValidationResult)
    assert status.status == "warn"
