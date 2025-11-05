from __future__ import annotations

import json
import re
from pathlib import Path
from types import SimpleNamespace

class _StubFileSystem:
    def __init__(self, existing_paths: set[str]) -> None:
        self._existing_paths = existing_paths

    def exists(self, path: object) -> bool:
        if hasattr(path, "toString"):
            return path.toString() in self._existing_paths
        return False


class _StubHadoopPath:
    def __init__(self, factory: "_StubPathFactory", value: str) -> None:
        self._factory = factory
        self._value = value

    def getFileSystem(self, _conf: object) -> _StubFileSystem:
        return self._factory.filesystem

    def toString(self) -> str:  # pragma: no cover - convenience for debugging
        return self._value


class _StubPathFactory:
    def __init__(self, existing_paths: set[str]) -> None:
        self.filesystem = _StubFileSystem(existing_paths)

    def __call__(self, base: object, child: str | None = None) -> _StubHadoopPath:
        if child is None:
            value = self._to_string(base)
        else:
            prefix = self._to_string(base).rstrip("/")
            value = f"{prefix}/{child}"
        return _StubHadoopPath(self, value)

    @staticmethod
    def _to_string(value: object) -> str:
        if hasattr(value, "toString"):
            return value.toString()
        return str(value)

from dc43_service_backends.governance.storage.delta import DeltaGovernanceStore
from dc43_service_clients.data_quality import ValidationResult


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


class _StubSqlResult:
    def __init__(self, exists: bool) -> None:
        self._exists = exists

    def collect(self) -> list[object]:
        if self._exists:
            return [SimpleNamespace(result=1)]
        return []


class _StubSpark:
    def __init__(
        self,
        *,
        existing_tables: set[str] | None = None,
        filesystem_paths: set[str] | None = None,
        information_schema: set[tuple[str, str, str]] | None = None,
    ) -> None:
        self._records: list[dict[str, object]] = []
        self._dataframes: list[dict[str, object]] = []
        self._schemas: list[object] = []
        self._existing_tables = existing_tables or set()
        self._information_schema = information_schema or set()
        self.catalog = SimpleNamespace(tableExists=self._table_exists)
        self._sql_queries: list[str] = []
        if filesystem_paths is not None:
            path_factory = _StubPathFactory(filesystem_paths)
            self._jvm = SimpleNamespace(
                org=SimpleNamespace(
                    apache=SimpleNamespace(
                        hadoop=SimpleNamespace(fs=SimpleNamespace(Path=path_factory))
                    )
                )
            )
            self._jsc = SimpleNamespace(hadoopConfiguration=lambda: SimpleNamespace())

    def _table_exists(self, name: str) -> bool:
        return name in self._existing_tables

    @property
    def records(self) -> list[dict[str, object]]:
        return self._records

    @property
    def schemas(self) -> list[object]:
        return self._schemas

    @property
    def sql_queries(self) -> list[str]:
        return self._sql_queries

    @property
    def dataframes(self) -> list[dict[str, object]]:
        return self._dataframes

    def createDataFrame(self, data: list[object], schema: object | None = None) -> _RecordingDataFrame:
        self._schemas.append(schema)
        self._dataframes.append({"data": list(data), "schema": schema})
        return _RecordingDataFrame(self._records)

    def sql(self, query: str) -> _StubSqlResult:
        self._sql_queries.append(query)
        if "system.information_schema.tables" not in query:
            return _StubSqlResult(False)
        matches = dict(re.findall(r"table_(catalog|schema|name)\s*=\s*'([^']*)'", query))
        catalog = matches.get("catalog")
        schema = matches.get("schema")
        name = matches.get("name")
        exists = (
            catalog is not None
            and schema is not None
            and name is not None
            and (catalog, schema, name) in self._information_schema
        )
        return _StubSqlResult(exists)


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
        metrics_table="analytics.governance.metrics",
    )

    tables = _tables(spark.records)
    assert {entry["target"] for entry in tables} == {
        "analytics.governance.status",
        "analytics.governance.activity",
        "analytics.governance.links",
        "analytics.governance.metrics",
    }
    for entry in tables:
        assert entry["format"] == "delta"
        assert entry["mode"] == "overwrite"
        assert entry["options"].get("overwriteSchema") == "true"


def test_bootstrap_derives_metrics_table_name() -> None:
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
        "analytics.governance.status_metrics",
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
        metrics_table="analytics.governance.metrics",
    )

    tables = _tables(spark.records)
    assert {entry["target"] for entry in tables} == {
        "analytics.governance.activity",
        "analytics.governance.links",
        "analytics.governance.metrics",
    }


def test_bootstrap_skips_tables_detected_via_information_schema() -> None:
    spark = _StubSpark(
        information_schema={("analytics", "governance", "status")}
    )
    DeltaGovernanceStore(
        spark,
        status_table="analytics.governance.status",
        activity_table="analytics.governance.activity",
        link_table="analytics.governance.links",
        metrics_table="analytics.governance.metrics",
    )

    tables = _tables(spark.records)
    assert {entry["target"] for entry in tables} == {
        "analytics.governance.activity",
        "analytics.governance.links",
        "analytics.governance.metrics",
    }
    assert any(
        "information_schema.tables" in query for query in spark.sql_queries
    )


def test_bootstrap_initialises_delta_folders(tmp_path: Path) -> None:
    spark = _StubSpark()
    DeltaGovernanceStore(spark, base_path=tmp_path)

    paths = _paths(spark.records)
    assert {Path(str(entry["target"])).name for entry in paths} == {
        "status",
        "links",
        "activity",
        "metrics",
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
        "metrics",
    }


def test_remote_delta_folder_detection_uses_spark_filesystem(tmp_path: Path) -> None:
    spark = _StubSpark(filesystem_paths={"s3://bucket/status/_delta_log"})
    store = DeltaGovernanceStore(spark, base_path=tmp_path, bootstrap_tables=False)

    store._ensure_delta_target(
        table=None,
        folder="s3://bucket/status",
        schema=DeltaGovernanceStore._STATUS_SCHEMA,
    )

    assert not spark.records


def test_save_status_records_metrics_entries(tmp_path: Path) -> None:
    spark = _StubSpark()
    store = DeltaGovernanceStore(spark, base_path=tmp_path)
    store._now = lambda: "2024-03-01T00:00:00Z"  # type: ignore[assignment]

    status = ValidationResult(status="ok", metrics={"violations.total": 3, "summary": {"passed": 10}})

    store.save_status(
        contract_id="contracts",
        contract_version="1.0.0",
        dataset_id="orders",
        dataset_version="2024-02-29",
        status=status,
    )

    metrics_frames = [
        frame
        for frame in spark.dataframes
        if frame["schema"] is DeltaGovernanceStore._METRIC_SCHEMA and frame["data"]
    ]
    assert len(metrics_frames) == 1
    rows = metrics_frames[0]["data"]
    assert {row["metric_key"] for row in rows} == {"violations.total", "summary"}
    total_row = next(row for row in rows if row["metric_key"] == "violations.total")
    assert total_row["metric_numeric_value"] == 3.0
    assert total_row["status_recorded_at"] == "2024-03-01T00:00:00Z"
    summary_row = next(row for row in rows if row["metric_key"] == "summary")
    assert json.loads(summary_row["metric_value"] or "{}") == {"passed": 10}
    assert summary_row["metric_numeric_value"] is None


def test_save_status_appends_metrics_to_table_target() -> None:
    spark = _StubSpark()
    store = DeltaGovernanceStore(
        spark,
        status_table="analytics.status",
        activity_table="analytics.activity",
        link_table="analytics.links",
        metrics_table="analytics.metrics",
    )
    store._now = lambda: "2024-04-05T12:00:00Z"  # type: ignore[assignment]

    status = ValidationResult(status="ok", metrics={"violations.total": 1})

    store.save_status(
        contract_id="contracts",
        contract_version="2.0.0",
        dataset_id="orders",
        dataset_version="2024-04-01",
        status=status,
    )

    metric_writes = [entry for entry in spark.records if entry["target"] == "analytics.metrics"]
    assert metric_writes
    assert all(entry["type"] == "table" for entry in metric_writes)


def test_save_status_appends_metrics_to_derived_table_target() -> None:
    spark = _StubSpark()
    store = DeltaGovernanceStore(
        spark,
        status_table="analytics.status",
        activity_table="analytics.activity",
        link_table="analytics.links",
    )
    store._now = lambda: "2024-04-05T12:00:00Z"  # type: ignore[assignment]

    status = ValidationResult(status="ok", metrics={"violations.total": 1})

    store.save_status(
        contract_id="contracts",
        contract_version="2.0.0",
        dataset_id="orders",
        dataset_version="2024-04-01",
        status=status,
    )

    metric_writes = [
        entry
        for entry in spark.records
        if entry["target"] == "analytics.status_metrics"
    ]
    assert metric_writes
    assert all(entry["type"] == "table" for entry in metric_writes)


def test_load_metrics_filters_results(tmp_path: Path) -> None:
    from pyspark.sql import SparkSession

    session = SparkSession.builder.master("local[1]").appName("dc43-tests").getOrCreate()
    try:
        spark = _StubSpark()
        store = DeltaGovernanceStore(spark, base_path=tmp_path, bootstrap_tables=False)

        rows = [
            {
                "dataset_id": "orders",
                "dataset_version": "2024-03-01",
                "contract_id": "contracts",
                "contract_version": "1.0.0",
                "status_recorded_at": "2024-03-02T00:00:00Z",
                "metric_key": "violations.total",
                "metric_value": "0",
                "metric_numeric_value": 0.0,
            },
            {
                "dataset_id": "orders",
                "dataset_version": "2024-03-02",
                "contract_id": "contracts",
                "contract_version": "1.0.0",
                "status_recorded_at": "2024-03-03T00:00:00Z",
                "metric_key": "summary",
                "metric_value": "{\"passed\": 10}",
                "metric_numeric_value": None,
            },
            {
                "dataset_id": "returns",
                "dataset_version": "2024-03-01",
                "contract_id": "contracts",
                "contract_version": "1.0.0",
                "status_recorded_at": "2024-03-02T00:00:00Z",
                "metric_key": "violations.total",
                "metric_value": "1",
                "metric_numeric_value": 1.0,
            },
        ]

        class _FakeRow(dict):
            def asDict(self, recursive: bool = False) -> dict[str, object]:
                return dict(self)

        class _FakeDataFrame:
            def __init__(self, data: list[dict[str, object]]) -> None:
                self._data = data

            def filter(self, _condition: object) -> "_FakeDataFrame":
                return self

            def orderBy(self, *_: object) -> "_FakeDataFrame":
                return self

            def collect(self) -> list[_FakeRow]:
                return [_FakeRow(row) for row in self._data]

        target_dataset_id = "orders"
        target_version = "2024-03-01"

        def _fake_read(**_: object) -> _FakeDataFrame:
            filtered = [
                row
                for row in rows
                if row["dataset_id"] == target_dataset_id and row["dataset_version"] == target_version
            ]
            return _FakeDataFrame(filtered)

        store._read = _fake_read  # type: ignore[assignment]

        entries = store.load_metrics(
            dataset_id=target_dataset_id,
            dataset_version=target_version,
        )
    finally:
        session.stop()

    assert len(entries) == 1
    entry = entries[0]
    assert entry["metric_key"] == "violations.total"
    assert entry["metric_numeric_value"] == 0.0
