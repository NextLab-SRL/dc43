from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

from dc43_service_backends.governance.storage.delta import DeltaGovernanceStore


class _RecordingWriter:
    def __init__(self, records: list[dict[str, object]]) -> None:
        self._records = records
        self._format: str | None = None
        self._mode: str | None = None
        self._options: dict[str, object] = {}

    def format(self, name: str) -> "_RecordingWriter":
        self._format = name
        return self

    def mode(self, name: str) -> "_RecordingWriter":
        self._mode = name
        return self

    def option(self, key: str, value: object) -> "_RecordingWriter":
        self._options[key] = value
        return self

    def saveAsTable(self, table: str) -> None:
        self._records.append(
            {
                "type": "table",
                "target": table,
                "format": self._format,
                "mode": self._mode,
                "options": dict(self._options),
            }
        )

    def save(self) -> None:
        self._records.append(
            {
                "type": "path",
                "target": self._options.get("path"),
                "format": self._format,
                "mode": self._mode,
                "options": dict(self._options),
            }
        )


class _RecordingDataFrame:
    def __init__(self, records: list[dict[str, object]]) -> None:
        self._records = records

    @property
    def write(self) -> _RecordingWriter:
        return _RecordingWriter(self._records)


class _StubSpark:
    def __init__(self, *, existing_tables: set[str] | None = None) -> None:
        self._records: list[dict[str, object]] = []
        self._schemas: list[object] = []
        self._existing_tables = existing_tables or set()
        self.catalog = SimpleNamespace(tableExists=self._table_exists)

    def _table_exists(self, name: str) -> bool:
        return name in self._existing_tables

    @property
    def records(self) -> list[dict[str, object]]:
        return self._records

    @property
    def schemas(self) -> list[object]:
        return self._schemas

    def createDataFrame(self, data: list[object], schema: object) -> _RecordingDataFrame:
        self._schemas.append(schema)
        return _RecordingDataFrame(self._records)


def _tables(records: list[dict[str, object]]) -> list[dict[str, object]]:
    return [entry for entry in records if entry["type"] == "table"]


def _paths(records: list[dict[str, object]]) -> list[dict[str, object]]:
    return [entry for entry in records if entry["type"] == "path"]


def test_bootstrap_creates_missing_delta_tables() -> None:
    spark = _StubSpark()
    DeltaGovernanceStore(
        spark,
        status_table="analytics.governance.status",
        activity_table="analytics.governance.activity",
        link_table="analytics.governance.links",
    )

    tables = _tables(spark.records)
    assert {entry["target"] for entry in tables} == {
        "analytics.governance.status",
        "analytics.governance.activity",
        "analytics.governance.links",
    }
    for entry in tables:
        assert entry["format"] == "delta"
        assert entry["mode"] == "overwrite"
        assert entry["options"].get("overwriteSchema") == "true"


def test_bootstrap_skips_existing_tables() -> None:
    spark = _StubSpark(existing_tables={"analytics.governance.status"})
    DeltaGovernanceStore(
        spark,
        status_table="analytics.governance.status",
        activity_table="analytics.governance.activity",
        link_table="analytics.governance.links",
    )

    tables = _tables(spark.records)
    assert {entry["target"] for entry in tables} == {
        "analytics.governance.activity",
        "analytics.governance.links",
    }


def test_bootstrap_initialises_delta_folders(tmp_path: Path) -> None:
    spark = _StubSpark()
    DeltaGovernanceStore(spark, base_path=tmp_path)

    paths = _paths(spark.records)
    assert {Path(str(entry["target"])).name for entry in paths} == {
        "status",
        "links",
        "activity",
    }
    for entry in paths:
        assert entry["format"] == "delta"
        assert entry["mode"] == "overwrite"
        assert entry["options"].get("overwriteSchema") == "true"


def test_bootstrap_skips_existing_delta_folders(tmp_path: Path) -> None:
    status_log = tmp_path / "status" / "_delta_log"
    status_log.mkdir(parents=True)

    spark = _StubSpark()
    DeltaGovernanceStore(spark, base_path=tmp_path)

    paths = _paths(spark.records)
    assert {Path(str(entry["target"])).name for entry in paths} == {
        "links",
        "activity",
    }
