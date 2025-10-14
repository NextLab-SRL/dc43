"""Unit tests for the DLT helpers."""

from __future__ import annotations

from types import SimpleNamespace
from typing import Iterable, Mapping, Sequence

import pytest

from dc43_integrations.spark import dlt_local
from dc43_integrations.spark.dlt import (
    DLTContractBinding,
    DLTExpectations,
    apply_dlt_expectations,
    contract_expectations,
    contract_table,
    contract_view,
    expectation_decorators,
    expectations_from_validation_details,
)
from open_data_contract_standard.model import OpenDataContractStandard, Description


class _SpyDLT(SimpleNamespace):
    """Minimal stub capturing calls to ``expect_all*``."""

    def __init__(self) -> None:  # pragma: no cover - exercised in tests
        super().__init__(calls=[])

    def expect_all(self, predicates):  # pragma: no cover - simple delegate
        self.calls.append(("expect_all", predicates))
        return lambda fn: fn

    def expect_all_or_drop(self, predicates):  # pragma: no cover - simple delegate
        self.calls.append(("expect_all_or_drop", predicates))
        return lambda fn: fn

    def table(self, **kwargs):  # pragma: no cover - simple delegate
        self.calls.append(("table", kwargs))

        def decorator(fn):
            self.calls.append(("table.apply", fn.__name__))
            return fn

        return decorator

    def view(self, **kwargs):  # pragma: no cover - simple delegate
        self.calls.append(("view", kwargs))

        def decorator(fn):
            self.calls.append(("view.apply", fn.__name__))
            return fn

        return decorator


def test_expectations_from_plan_separates_optional_rules():
    plan = [
        {"key": "required_column", "predicate": "col IS NOT NULL", "optional": False},
        {"key": "optional_enum", "predicate": "col IN ('a', 'b')", "optional": True},
        {"key": "missing_predicate", "optional": False},
    ]
    fallback = {"missing_predicate": "some_expression"}

    expectations = DLTExpectations.from_expectation_plan(plan, fallback_predicates=fallback)

    assert expectations.enforced == {"required_column": "col IS NOT NULL", "missing_predicate": "some_expression"}
    assert expectations.observed == {"optional_enum": "col IN ('a', 'b')"}


def test_ensure_dlt_module_falls_back_to_stub(monkeypatch):
    monkeypatch.setattr(dlt_local, "databricks_dlt", None, raising=False)
    monkeypatch.setattr(dlt_local, "_DLT_IMPORT_ERROR", ModuleNotFoundError("missing"), raising=False)
    monkeypatch.setattr(dlt_local, "_STUB_DLT_MODULE", None, raising=False)

    module = dlt_local.ensure_dlt_module(allow_stub=True)

    assert getattr(module, "__dc43_is_stub__", False)

    @module.table
    def sample():  # pragma: no cover - simple stub
        return "ok"

    assert sample() == "ok"


def test_apply_dlt_expectations_accepts_mapping():
    dlt = _SpyDLT()
    apply_dlt_expectations(dlt, {"rule": "predicate"}, drop=True)

    assert dlt.calls == [("expect_all_or_drop", {"rule": "predicate"})]


def test_expectation_decorators_return_stackable_callables():
    dlt = _SpyDLT()
    expectations = DLTExpectations(enforced={"required": "predicate"}, observed={"optional": "expr"})

    decorators = expectation_decorators(dlt, expectations)

    assert len(decorators) == 2
    for decorator in decorators:
        assert callable(decorator)


def test_expectations_from_validation_details_prefers_plan():
    plan = [{"key": "critical", "predicate": "x > 0"}]
    details = {
        "expectation_plan": plan,
        "expectation_predicates": {"critical": "ignored", "other": "y > 0"},
    }

    expectations = expectations_from_validation_details(details)

    assert expectations.enforced == {"critical": "x > 0"}
    assert expectations.observed == {}


@pytest.mark.parametrize(
    "details",
    [
        {},
        {"expectation_predicates": {}},
        {"expectation_plan": []},
    ],
)
def test_expectations_from_validation_details_empty(details):
    expectations = expectations_from_validation_details(details)

    assert expectations.enforced == {}
    assert expectations.observed == {}


def _make_contract(version: str = "1.0.0") -> OpenDataContractStandard:
    return OpenDataContractStandard(
        id="demo.contract",
        version=version,
        kind="DataContract",
        apiVersion="3.0.2",
        name="Demo",
        description=Description(usage="Demo contract"),
    )


class _StubContractService:
    def __init__(self, contracts: Sequence[OpenDataContractStandard]) -> None:
        self.contracts = {(c.id, c.version): c for c in contracts}
        self.calls: list[tuple[str, tuple[str, ...]]] = []

    def get(self, contract_id: str, contract_version: str) -> OpenDataContractStandard:
        self.calls.append(("get", (contract_id, contract_version)))
        return self.contracts[(contract_id, contract_version)]

    def latest(self, contract_id: str) -> OpenDataContractStandard:
        self.calls.append(("latest", (contract_id,)))
        versions = sorted(v for (cid, v) in self.contracts if cid == contract_id)
        return self.contracts[(contract_id, versions[-1])]

    def list_versions(self, contract_id: str) -> Sequence[str]:
        self.calls.append(("list_versions", (contract_id,)))
        return [v for (cid, v) in sorted(self.contracts) if cid == contract_id]


class _StubDataQualityService:
    def __init__(self, plan: Iterable[Mapping[str, object]]) -> None:
        self.plan = list(plan)
        self.calls: list[tuple[str, str]] = []

    def describe_expectations(self, *, contract: OpenDataContractStandard):
        self.calls.append((contract.id, contract.version))
        return list(self.plan)


def test_contract_expectations_registers_dlt_annotations():
    contract = _make_contract("1.2.0")
    plan = [
        {"key": "required", "predicate": "x IS NOT NULL"},
        {"key": "optional", "predicate": "y > 0", "optional": True},
    ]
    service = _StubContractService([contract])
    dq = _StubDataQualityService(plan)
    dlt = _SpyDLT()

    decorator = contract_expectations(
        dlt,
        contract_id=contract.id,
        contract_service=service,
        data_quality_service=dq,
        expected_contract_version="==1.2.0",
    )

    @decorator
    def asset():
        return "df"

    assert dlt.calls == [
        ("expect_all_or_drop", {"required": "x IS NOT NULL"}),
        ("expect_all", {"optional": "y > 0"}),
    ]
    assert (contract.id, contract.version) == getattr(asset, "__dc43_contract__")
    binding = getattr(asset, "__dc43_contract_binding__")
    assert isinstance(binding, DLTContractBinding)
    assert binding.contract_version == "1.2.0"
    assert binding.expectations.enforced == {"required": "x IS NOT NULL"}
    assert binding.expectation_plan[0]["key"] == "required"


def test_contract_expectations_supports_minimum_version_selector():
    contract_old = _make_contract("1.0.0")
    contract_new = _make_contract("1.3.0")
    plan = [{"key": "rule", "predicate": "x > 0"}]
    service = _StubContractService([contract_old, contract_new])
    dq = _StubDataQualityService(plan)
    dlt = _SpyDLT()

    decorator = contract_expectations(
        dlt,
        contract_id="demo.contract",
        contract_service=service,
        data_quality_service=dq,
        expected_contract_version=">=1.1.0",
    )

    @decorator
    def asset():
        return "df"

    binding = getattr(asset, "__dc43_contract_binding__")
    assert binding.contract_version == "1.3.0"
    assert ("list_versions", ("demo.contract",)) in service.calls


def test_contract_table_wraps_dlt_table_and_preserves_binding():
    contract = _make_contract("2.0.0")
    plan = [{"key": "rule", "predicate": "amount > 0"}]
    service = _StubContractService([contract])
    dq = _StubDataQualityService(plan)
    dlt = _SpyDLT()

    decorator = contract_table(
        dlt,
        name="orders",
        contract_id=contract.id,
        contract_service=service,
        data_quality_service=dq,
    )

    @decorator
    def asset():
        return "df"

    assert ("table", {"name": "orders"}) in dlt.calls
    assert ("table.apply", "asset") in dlt.calls
    assert getattr(asset, "__dc43_contract__") == (contract.id, contract.version)
    binding = getattr(asset, "__dc43_contract_binding__")
    assert binding.expectations.enforced == {"rule": "amount > 0"}


def test_contract_view_wraps_dlt_view():
    contract = _make_contract("3.1.0")
    plan = [{"key": "rule", "predicate": "status IN ('OK')", "optional": True}]
    service = _StubContractService([contract])
    dq = _StubDataQualityService(plan)
    dlt = _SpyDLT()

    decorator = contract_view(
        dlt,
        materialized="false",
        contract_id=contract.id,
        contract_service=service,
        data_quality_service=dq,
    )

    @decorator
    def asset():
        return "df"

    assert ("view", {"materialized": "false"}) in dlt.calls
    assert ("view.apply", "asset") in dlt.calls
    binding = getattr(asset, "__dc43_contract_binding__")
    assert binding.expectations.observed == {"rule": "status IN ('OK')"}
