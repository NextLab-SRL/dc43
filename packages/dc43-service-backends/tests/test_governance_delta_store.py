from __future__ import annotations

import re
from pathlib import Path
from types import SimpleNamespace
import sys
import types


def _install_pyspark_stub() -> None:
    if "pyspark" in sys.modules:
        return

    pyspark = types.ModuleType("pyspark")
    sql_module = types.ModuleType("pyspark.sql")

    class _StubSparkSession:  # pragma: no cover - attribute container only
        pass

    class _StubDataFrame:  # pragma: no cover - attribute container only
        pass

    sql_module.SparkSession = _StubSparkSession
    sql_module.DataFrame = _StubDataFrame

    functions_module = types.ModuleType("pyspark.sql.functions")

    class _StubColumn:  # pragma: no cover - attribute container only
        def __init__(self, name: str) -> None:
            self.name = name

    def _col(name: str) -> _StubColumn:
        return _StubColumn(name)

    functions_module.col = _col

    types_module = types.ModuleType("pyspark.sql.types")

    class _StubStructField(tuple):
        def __new__(cls, name: str, data_type: object, nullable: bool) -> "_StubStructField":
            return tuple.__new__(cls, (name, data_type, nullable))

    class _StubStructType(list):
        def __init__(self, fields: list[_StubStructField]):
            super().__init__(fields)

    def _boolean_type() -> str:
        return "boolean"

    def _string_type() -> str:
        return "string"

    types_module.StructField = _StubStructField
    types_module.StructType = _StubStructType
    types_module.BooleanType = _boolean_type
    types_module.StringType = _string_type

    utils_module = types.ModuleType("pyspark.sql.utils")

    class _StubAnalysisException(Exception):
        pass

    utils_module.AnalysisException = _StubAnalysisException

    pyspark.sql = sql_module
    sql_module.functions = functions_module
    sql_module.types = types_module
    sql_module.utils = utils_module

    sys.modules["pyspark"] = pyspark
    sys.modules["pyspark.sql"] = sql_module
    sys.modules["pyspark.sql.functions"] = functions_module
    sys.modules["pyspark.sql.types"] = types_module
    sys.modules["pyspark.sql.utils"] = utils_module


_install_pyspark_stub()


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

    def createDataFrame(self, data: list[object], schema: object) -> _RecordingDataFrame:
        self._schemas.append(schema)
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


def test_bootstrap_skips_tables_detected_via_information_schema() -> None:
    spark = _StubSpark(
        information_schema={("analytics", "governance", "status")}
    )
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


def test_remote_delta_folder_detection_uses_spark_filesystem(tmp_path: Path) -> None:
    spark = _StubSpark(filesystem_paths={"s3://bucket/status/_delta_log"})
    store = DeltaGovernanceStore(spark, base_path=tmp_path, bootstrap_tables=False)

    store._ensure_delta_target(
        table=None,
        folder="s3://bucket/status",
        schema=DeltaGovernanceStore._STATUS_SCHEMA,
    )

    assert not spark.records
