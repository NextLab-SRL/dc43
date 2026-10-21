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


def test_sql_store_record_pipeline_event_retries_on_delta_concurrency_conflict(
    sql_engine, monkeypatch: pytest.MonkeyPatch
) -> None:
    from sqlalchemy.exc import DatabaseError

    store = SQLGovernanceStore(sql_engine, max_retries=3, retry_backoff=0.0)
    store.record_pipeline_event(
        contract_id="sales.orders",
        contract_version="1.0.0",
        dataset_id="01a0249f-5930-751b-b72e-7782ed3e323a",
        dataset_version="ENTETE_ACORES_Dev 0.0.2",
        event={"operation": "read", "recorded_at": "2026-10-21T16:28:00Z"},
    )

    original_write = store._write_payload
    attempts = 0

    def flaky_write(table, *, dataset_id, dataset_version, payload, extra=None):
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            # Simulate a concurrent process committing version 6741 before our DELETE succeeds
            original_write(
                table,
                dataset_id=dataset_id,
                dataset_version=dataset_version,
                payload={
                    "dataset_id": dataset_id,
                    "dataset_version": dataset_version,
                    "contract_id": "sales.orders",
                    "contract_version": "1.0.0",
                    "events": [
                        {"operation": "read", "recorded_at": "2026-10-21T16:28:00Z"},
                        {"operation": "concurrent_write", "recorded_at": "2026-10-21T16:28:01Z"},
                    ],
                },
                extra=extra,
            )
            raise DatabaseError(
                statement="DELETE FROM dq_activity WHERE dq_activity.dataset_id = :dataset_id_1 AND dq_activity.dataset_version = :dataset_version_1",
                params={"dataset_id_1": dataset_id, "dataset_version_1": dataset_version},
                orig=Exception(
                    "[DELTA_CONCURRENT_DELETE_READ.ROW_LEVEL_CHANGES] Transaction conflict detected, "
                    "a concurrent DELETE deleted data from table cat_dd_fraude_dev_private.governance.dq_activity "
                    "(committed at version 6741) that this transaction read."
                ),
            )
        return original_write(
            table,
            dataset_id=dataset_id,
            dataset_version=dataset_version,
            payload=payload,
            extra=extra,
        )

    monkeypatch.setattr(store, "_write_payload", flaky_write)

    store.record_pipeline_event(
        contract_id="sales.orders",
        contract_version="1.0.0",
        dataset_id="01a0249f-5930-751b-b72e-7782ed3e323a",
        dataset_version="ENTETE_ACORES_Dev 0.0.2",
        event={"operation": "write", "recorded_at": "2026-10-21T16:28:02Z"},
    )

    assert attempts == 2
    activity = store.load_pipeline_activity(
        dataset_id="01a0249f-5930-751b-b72e-7782ed3e323a",
        dataset_version="ENTETE_ACORES_Dev 0.0.2",
    )
    assert len(activity) == 1
    events = activity[0]["events"]
    assert isinstance(events, list)
    assert [item["operation"] for item in events] == [
        "read",
        "concurrent_write",
        "write",
    ]

