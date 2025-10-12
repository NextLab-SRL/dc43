from __future__ import annotations

from types import SimpleNamespace

import pytest

from dc43_service_backends.governance.storage.delta import DeltaGovernanceStatusStore


class _FakeSpark:
    def __init__(self, rows: list[SimpleNamespace]) -> None:
        self.rows = rows
        self.queries: list[str] = []

    def sql(self, query: str) -> "_FakeDataFrame":  # pragma: no cover - behaviour verified via tests
        self.queries.append(query)
        return _FakeDataFrame(self.rows)


class _FakeDataFrame:
    def __init__(self, rows: list[SimpleNamespace]) -> None:
        self._rows = rows

    def head(self, limit: int) -> list[SimpleNamespace]:
        return self._rows[:limit]


def test_load_status_normalises_details() -> None:
    rows = [
        SimpleNamespace(
            status="warn",
            reason="threshold",
            details=[("violations", 3), ("notes", "check metrics")],
            metrics={"violations.count": 3},
            schema={"orders": {"id": "int"}},
        )
    ]
    spark = _FakeSpark(rows)
    store = DeltaGovernanceStatusStore(spark, table="governance.statuses")

    status = store.load_status(dataset_id="table:orders", dataset_version="1")

    assert status.status == "warn"
    assert status.reason == "threshold"
    assert status.details["violations"] == 3
    assert status.details["notes"] == "check metrics"
    assert status.metrics == {"violations.count": 3}
    assert status.schema == {"orders": {"id": "int"}}
    assert "FROM governance.statuses" in spark.queries[0]


def test_load_status_missing_row() -> None:
    spark = _FakeSpark([])
    store = DeltaGovernanceStatusStore(spark, table="governance.statuses")

    with pytest.raises(LookupError):
        store.load_status(dataset_id="table:orders", dataset_version="missing")
