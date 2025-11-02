from __future__ import annotations

from pathlib import Path

import pytest

pytest.importorskip("sqlalchemy")

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
