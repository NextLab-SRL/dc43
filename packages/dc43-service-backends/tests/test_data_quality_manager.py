from types import SimpleNamespace

import pytest

from dc43_service_backends.data_quality.backend.manager import DataQualityManager
from dc43_service_clients.data_quality import ObservationPayload, ValidationResult


class _StubEngine:
    def __init__(self, name: str) -> None:
        self.name = name
        self.calls: list[tuple[str, object]] = []

    def evaluate(self, contract: object, payload: ObservationPayload) -> ValidationResult:
        self.calls.append(("evaluate", contract))
        return ValidationResult(status=self.name, details={"engine": self.name})

    def describe_expectations(self, contract: object) -> list[dict[str, object]]:
        self.calls.append(("describe", contract))
        return [{"engine": self.name}]


def _contract_with_engine(engine: str | None = None) -> object:
    metadata = {"quality_engine": engine} if engine else {}
    schema_object = SimpleNamespace(quality=[])
    return SimpleNamespace(metadata=metadata, schema_=[schema_object])


def test_manager_uses_default_engine_when_contract_omits_override() -> None:
    engine = _StubEngine("soda")
    manager = DataQualityManager(default_engine="soda", engines={"soda": engine})

    contract = _contract_with_engine()
    payload = ObservationPayload(metrics={})
    result = manager.evaluate(contract, payload)

    assert result.status == "soda"
    assert engine.calls and engine.calls[0][0] == "evaluate"


def test_manager_prefers_contract_metadata_engine() -> None:
    engine = _StubEngine("great_expectations")
    manager = DataQualityManager(
        default_engine="native",
        engines={"great_expectations": engine},
    )

    contract = _contract_with_engine("great_expectations")
    payload = ObservationPayload(metrics={"great_expectations": {"status": "ok"}})
    result = manager.evaluate(contract, payload)

    assert result.status == "great_expectations"
    assert engine.calls and engine.calls[0][0] == "evaluate"


def test_manager_raises_for_unknown_engine() -> None:
    manager = DataQualityManager(default_engine="custom")
    contract = _contract_with_engine("nonexistent")
    with pytest.raises(RuntimeError):
        manager.evaluate(contract, ObservationPayload(metrics={}))
