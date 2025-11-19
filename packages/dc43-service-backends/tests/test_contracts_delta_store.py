import json
from pathlib import Path

from dc43_service_backends.contracts.backend.stores.delta import DeltaContractStore
from open_data_contract_standard.model import (
    Description,
    OpenDataContractStandard,
    SchemaObject,
    SchemaProperty,
)


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
        if statement.startswith("SELECT version, json"):
            contract_id = self._extract(statement, "contract_id = '")
            rows = [
                (version, record["json"])
                for (stored_id, version), record in self.storage.items()
                if stored_id == contract_id
            ]
            return _FakeSparkResult(rows)
        if statement.startswith("SELECT json FROM"):
            contract_id = self._extract(statement, "contract_id = '")
            version = self._extract(statement, "version = '", after="AND version")
            record = self.storage.get((contract_id, version))
            rows: list[tuple] = []
            if record:
                rows.append((record["json"],))
            return _FakeSparkResult(rows)
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


def _contract(version: str) -> OpenDataContractStandard:
    return OpenDataContractStandard(
        version=version,
        kind="DataContract",
        apiVersion="3.0.2",
        id="sales.orders",
        name="Orders",
        description=Description(usage="Orders facts"),
        schema=[
            SchemaObject(
                name="orders",
                properties=[
                    SchemaProperty(
                        name="order_id", physicalType="bigint", required=True
                    ),
                ],
            )
        ],
    )


def _encode(contract: OpenDataContractStandard) -> str:
    payload = contract.model_dump(by_alias=True, exclude_none=True)
    return json.dumps(payload, separators=(",", ":"))


def test_delta_contract_store_prefers_release_over_rc(tmp_path: Path) -> None:
    spark = _FakeSpark()
    store = DeltaContractStore(spark, path=str(tmp_path / "contracts"))

    rc_contract = _contract("0.32.0.0rc2")
    release_contract = _contract("0.32.0.0")

    spark.storage[(rc_contract.id, rc_contract.version)] = {
        "json": _encode(rc_contract)
    }
    spark.storage[(release_contract.id, release_contract.version)] = {
        "json": _encode(release_contract)
    }

    latest = store.latest("sales.orders")

    assert latest is not None
    assert latest.version == "0.32.0.0"


def test_delta_contract_store_ignores_blank_versions(tmp_path: Path) -> None:
    spark = _FakeSpark()
    store = DeltaContractStore(spark, path=str(tmp_path / "contracts"))

    blank = _contract("0.0.0")
    final = _contract("1.0.0")

    spark.storage[(blank.id, "")] = {"json": _encode(blank)}
    spark.storage[(final.id, final.version)] = {"json": _encode(final)}

    latest = store.latest("sales.orders")

    assert latest is not None
    assert latest.version == "1.0.0"
