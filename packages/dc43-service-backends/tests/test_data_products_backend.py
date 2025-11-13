import json
from pathlib import Path

import pytest

from dc43_service_backends.core.versioning import version_key
from dc43_service_backends.data_products import (
    CollibraDataProductServiceBackend,
    DeltaDataProductServiceBackend,
    FilesystemDataProductServiceBackend,
    LocalDataProductServiceBackend,
    StubCollibraDataProductAdapter,
)
from dc43_service_clients.odps import (
    DataProductInputPort,
    DataProductOutputPort,
    OpenDataProductStandard,
)


def test_local_backend_accepts_pydantic_like_payload() -> None:
    backend = LocalDataProductServiceBackend()

    class _PydanticLikeProduct:
        def __init__(self) -> None:
            self.id = "dp.sales"
            self.status = "active"
            self.version = "1.0.0"

        def model_dump(self, **_: object) -> dict[str, object]:
            return {
                "apiVersion": "1.0.0",
                "kind": "DataProduct",
                "id": self.id,
                "status": self.status,
                "version": self.version,
                "name": "Sales",
                "description": {"purpose": "Provide Sales Information"},
                "outputPorts": [
                    {
                        "name": "orders",
                        "version": "1.0.0",
                        "contractId": "contracts.orders",
                    }
                ],
            }

    backend.put(_PydanticLikeProduct())

    stored = backend.get("dp.sales", "1.0.0")
    assert stored.name == "Sales"
    assert stored.output_ports[0].contract_id == "contracts.orders"
    assert callable(getattr(stored, "clone"))


def test_local_backend_rejects_invalid_payload() -> None:
    backend = LocalDataProductServiceBackend()

    class _ForeignPayload:
        def __init__(self) -> None:
            self.id = "dp.sales.orders"
            self.status = "active"
            self.version = "1.0.0"

        def clone(self) -> "_ForeignPayload":
            return self

        def to_dict(self) -> dict[str, object]:
            return {
                "apiVersion": "3.0.2",
                "kind": "DataProduct",
                "id": self.id,
                "status": self.status,
            }

    with pytest.raises(ValueError):
        backend.put(_ForeignPayload())


def test_local_backend_lists_products() -> None:
    backend = LocalDataProductServiceBackend()
    backend.register_output_port(
        data_product_id="dp.analytics",
        port=DataProductOutputPort(
            name="snapshot", version="1.0.0", contract_id="snapshot"
        ),
    )

    listing = backend.list_data_products()
    assert list(listing.items) == ["dp.analytics"]
    assert listing.total == 1


def test_register_input_port_creates_draft_version() -> None:
    backend = LocalDataProductServiceBackend()

    registration = backend.register_input_port(
        data_product_id="dp.analytics",
        port=DataProductInputPort(name="orders", version="1.0.0", contract_id="orders"),
        source_data_product="dp.source",
        source_output_port="gold",
    )

    assert registration.changed is True
    product = registration.product
    assert product.version is not None
    assert product.status == "draft"
    assert product.input_ports[0].contract_id == "orders"
    latest = backend.latest("dp.analytics")
    assert latest is not None
    assert latest.version == product.version


def test_register_input_port_idempotent() -> None:
    backend = LocalDataProductServiceBackend()
    backend.register_input_port(
        data_product_id="dp.analytics",
        port=DataProductInputPort(name="orders", version="1.0.0", contract_id="orders"),
    )

    registration = backend.register_input_port(
        data_product_id="dp.analytics",
        port=DataProductInputPort(name="orders", version="1.0.0", contract_id="orders"),
    )

    assert registration.changed is False


def test_register_output_port_updates_version_once() -> None:
    backend = LocalDataProductServiceBackend()
    backend.register_output_port(
        data_product_id="dp.analytics",
        port=DataProductOutputPort(
            name="snapshot", version="1.0.0", contract_id="snapshot"
        ),
    )
    first_version = backend.latest("dp.analytics").version
    update = backend.register_output_port(
        data_product_id="dp.analytics",
        port=DataProductOutputPort(
            name="snapshot", version="1.1.0", contract_id="snapshot"
        ),
    )
    assert update.changed is True
    second_version = backend.latest("dp.analytics").version
    assert second_version != first_version


def test_resolve_output_contract_returns_contract_reference() -> None:
    backend = LocalDataProductServiceBackend()
    backend.register_output_port(
        data_product_id="dp.analytics",
        port=DataProductOutputPort(
            name="report", version="2.0.0", contract_id="report"
        ),
    )
    resolved = backend.resolve_output_contract(
        data_product_id="dp.analytics",
        port_name="report",
    )
    assert resolved == ("report", "2.0.0")


def test_filesystem_backend_persists_products(tmp_path: Path) -> None:
    backend = FilesystemDataProductServiceBackend(tmp_path)

    backend.register_output_port(
        data_product_id="dp.analytics",
        port=DataProductOutputPort(
            name="snapshot", version="1.0.0", contract_id="snapshot"
        ),
    )

    on_disk = list(tmp_path.rglob("*.json"))
    assert on_disk

    reloaded = FilesystemDataProductServiceBackend(tmp_path)
    product = reloaded.latest("dp.analytics")
    assert product is not None
    assert product.output_ports[0].contract_id == "snapshot"


def test_filesystem_backend_does_not_cache_state(tmp_path: Path) -> None:
    backend = FilesystemDataProductServiceBackend(tmp_path)

    assert not hasattr(backend, "_products")

    backend.register_output_port(
        data_product_id="dp.analytics",
        port=DataProductOutputPort(
            name="snapshot", version="1.0.0", contract_id="snapshot"
        ),
    )

    latest = backend.latest("dp.analytics")
    assert latest is not None
    assert latest.output_ports[0].contract_id == "snapshot"


def test_collibra_backend_uses_stub_adapter(tmp_path: Path) -> None:
    adapter = StubCollibraDataProductAdapter(str(tmp_path))
    backend = CollibraDataProductServiceBackend(adapter)

    assert not hasattr(backend, "_products")

    registration = backend.register_output_port(
        data_product_id="dp.analytics",
        port=DataProductOutputPort(
            name="snapshot", version="1.0.0", contract_id="snapshot"
        ),
    )

    assert registration.changed is True
    latest = backend.latest("dp.analytics")
    assert latest is not None
    assert latest.output_ports[0].contract_id == "snapshot"

    adapter.close()


class _FakeSparkResult:
    def __init__(self, rows: list[tuple]) -> None:
        self._rows = rows

    def collect(self) -> list[tuple]:
        return list(self._rows)

    def head(self, n: int) -> list[tuple]:
        return list(self._rows[:n])


class _FakeSpark:
    def __init__(self) -> None:
        self.storage: dict[tuple[str, str], dict[str, str]] = {}

    def sql(self, statement: str) -> _FakeSparkResult:
        statement = statement.strip()
        if statement.startswith("CREATE TABLE"):
            return _FakeSparkResult([])
        if statement.startswith("MERGE INTO"):
            parts = statement.split("'")
            data_product_id = parts[1]
            version = parts[3]
            status = parts[5]
            json_payload = parts[7]
            self.storage[(data_product_id, version)] = {
                "status": status,
                "json": json_payload,
            }
            return _FakeSparkResult([])
        if statement.startswith("SELECT data_product_id"):
            rows = [
                (data_product_id, version, record["json"])
                for (data_product_id, version), record in self.storage.items()
            ]
            return _FakeSparkResult(rows)
        if statement.startswith("SELECT version FROM"):
            data_product_id = self._extract(statement, "data_product_id = '")
            rows = [
                (version,)
                for (stored_id, version), _ in self.storage.items()
                if stored_id == data_product_id
            ]
            return _FakeSparkResult(rows)
        if statement.startswith("SELECT version, json"):
            data_product_id = self._extract(statement, "data_product_id = '")
            rows = [
                (version, record["json"])
                for (stored_id, version), record in self.storage.items()
                if stored_id == data_product_id
            ]
            return _FakeSparkResult(rows)
        if statement.startswith("SELECT json FROM"):
            data_product_id = self._extract(statement, "data_product_id = '")
            if "AND version" in statement:
                version = self._extract(statement, "version = '", after="AND version")
                record = self.storage.get((data_product_id, version))
                rows: list[tuple] = []
                if record:
                    rows.append((record["json"],))
                return _FakeSparkResult(rows)
            candidates = [
                (version, record["json"])
                for (stored_id, version), record in self.storage.items()
                if stored_id == data_product_id
            ]
            if not candidates:
                return _FakeSparkResult([])
            candidates.sort(key=lambda item: version_key(item[0]), reverse=True)
            return _FakeSparkResult([(candidates[0][1],)])
        return _FakeSparkResult([])

    @staticmethod
    def _extract(statement: str, marker: str, *, after: str | None = None) -> str:
        start = statement.find(
            marker if after is None else marker, statement.find(after) if after else 0
        )
        if start == -1:
            return ""
        start += len(marker)
        end = statement.find("'", start)
        return statement[start:end]


def test_delta_backend_prefers_release_over_rc(tmp_path: Path) -> None:
    spark = _FakeSpark()
    backend = DeltaDataProductServiceBackend(spark, path=str(tmp_path / "dp"))

    backend.put(
        OpenDataProductStandard(
            id="dp.sales",
            status="draft",
            version="0.31.0.0rc3",
            name="Sales",
            description={"purpose": "Provide Sales Information"},
            output_ports=[
                DataProductOutputPort(
                    name="orders",
                    version="0.31.0.0rc3",
                    contract_id="sales",
                )
            ],
        )
    )
    backend.put(
        OpenDataProductStandard(
            id="dp.sales",
            status="active",
            version="0.31.0.0",
            name="Sales",
            description={"purpose": "Provide Sales Information"},
            output_ports=[
                DataProductOutputPort(
                    name="orders",
                    version="0.31.0.0",
                    contract_id="sales",
                )
            ],
        )
    )

    latest = backend.latest("dp.sales")

    assert latest is not None
    assert latest.version == "0.31.0.0"


def test_version_key_orders_pre_releases() -> None:
    assert version_key("0.31.0.0dev1") < version_key("0.31.0.0rc1")
    assert version_key("0.31.0.0rc1") < version_key("0.31.0.0")
    assert version_key("0.31.0.0rc2") < version_key("0.31.0.0rc10")
    assert version_key("0.31.0.0draft2") < version_key("0.31.0.0draft10")
    assert version_key("0.31.0.0draft1") < version_key("0.31.0.0rc1")


def test_delta_backend_uses_spark_sql(tmp_path: Path) -> None:
    spark = _FakeSpark()
    backend = DeltaDataProductServiceBackend(spark, path=str(tmp_path / "dp"))

    assert not hasattr(backend, "_products")

    backend.register_output_port(
        data_product_id="dp.analytics",
        port=DataProductOutputPort(
            name="snapshot", version="1.0.0", contract_id="snapshot"
        ),
    )

    latest = backend.latest("dp.analytics")
    assert latest is not None
    assert ("dp.analytics", latest.version) in spark.storage

    fetched = backend.get("dp.analytics", latest.version)
    assert fetched is not None
    assert fetched.output_ports[0].contract_id == "snapshot"


def test_delta_backend_skips_invalid_rows(tmp_path: Path) -> None:
    spark = _FakeSpark()
    spark.storage[("dp.sales", "1.0.0")] = {
        "status": "active",
        "json": json.dumps(
            {
                "apiVersion": "3.0.2",
                "kind": "DataProduct",
                "id": "dp.sales",
                "status": "active",
            }
        ),
    }

    backend = DeltaDataProductServiceBackend(spark, path=str(tmp_path / "dp"))

    listing = backend.list_data_products()
    assert list(listing.items) == []


def test_delta_backend_ignores_blank_versions(tmp_path: Path) -> None:
    spark = _FakeSpark()
    spark.storage[("dp.sales", "")] = {
        "status": "draft",
        "json": json.dumps(
            {
                "apiVersion": "1.0.0",
                "kind": "DataProduct",
                "id": "dp.sales",
                "status": "draft",
            }
        ),
    }
    spark.storage[("dp.sales", "1.0.0")] = {
        "status": "active",
        "json": json.dumps(
            {
                "apiVersion": "1.0.0",
                "kind": "DataProduct",
                "id": "dp.sales",
                "status": "active",
                "version": "1.0.0",
            }
        ),
    }

    backend = DeltaDataProductServiceBackend(spark, path=str(tmp_path / "dp"))

    latest = backend.latest("dp.sales")
    assert latest is not None
    assert latest.version == "1.0.0"
    assert backend.list_versions("dp.sales") == ["1.0.0"]


def test_sql_store_persists_products() -> None:
    sqlalchemy = pytest.importorskip("sqlalchemy")
    from dc43_service_backends.data_products.backend.stores.sql import (
        SQLDataProductStore,
    )

    engine = sqlalchemy.create_engine("sqlite://")
    backend = LocalDataProductServiceBackend(store=SQLDataProductStore(engine))

    backend.register_output_port(
        data_product_id="dp.analytics",
        port=DataProductOutputPort(
            name="snapshot", version="1.0.0", contract_id="snapshot"
        ),
    )

    reloaded = LocalDataProductServiceBackend(store=SQLDataProductStore(engine))
    product = reloaded.latest("dp.analytics")
    assert product is not None
    assert product.output_ports[0].contract_id == "snapshot"

    listing = reloaded.list_data_products()
    assert "dp.analytics" in list(listing.items)
