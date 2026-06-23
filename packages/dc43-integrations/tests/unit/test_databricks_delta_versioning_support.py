"""Unit coverage for the Databricks versioning helpers."""

from dc43_integrations.examples.databricks_delta_versioning_support import (
    VersionedWriteSpec,
    _ensure_dataset_version,
    build_contract,
)


def _make_contract(version: str = "0.1.0"):
    return build_contract(
        version=version,
        contract_id="contracts.demo.orders",
        table_name="main.demo.orders",
        catalog="main",
        schema="demo",
        allowed_currencies=["USD"],
        include_discount=False,
    )


def test_ensure_dataset_version_preserves_existing_value():
    contract = _make_contract()
    spec = VersionedWriteSpec(contract=contract, dataset_version="1.0.0", rows=[])

    def fail_clock():  # pragma: no cover - should not be invoked
        raise AssertionError("Clock should not be evaluated when dataset_version is set")

    resolved = _ensure_dataset_version(spec, fail_clock)

    assert resolved == "1.0.0"
    assert spec.dataset_version == "1.0.0"


def test_ensure_dataset_version_generates_timestamp_when_missing():
    contract = _make_contract()
    spec = VersionedWriteSpec(contract=contract, dataset_version=None, rows=[])

    calls: list[str] = []

    def fake_clock() -> str:
        calls.append("tick")
        return "2024-05-01T00:00:00Z"

    resolved = _ensure_dataset_version(spec, fake_clock)

    assert resolved == "2024-05-01T00:00:00Z"
    assert spec.dataset_version == "2024-05-01T00:00:00Z"
    assert calls == ["tick"]
