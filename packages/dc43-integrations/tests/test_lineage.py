from __future__ import annotations

from pathlib import Path
from typing import Mapping

import pytest

pytest.importorskip(
    "openlineage.client.run", reason="openlineage-python is required for lineage integration tests"
)

from dc43_integrations.spark.open_data_lineage import build_lineage_run_event
from .helpers.orders import build_orders_contract, materialise_orders
from dc43_integrations.spark.io import (
    GovernanceSparkReadRequest,
    GovernanceSparkWriteRequest,
    read_with_governance,
    write_with_governance,
)
from dc43_service_clients.data_products import DataProductInputBinding, DataProductOutputBinding
from dc43_service_clients.data_quality import ValidationResult
from dc43_service_clients.governance.models import (
    GovernanceReadContext,
    GovernanceWriteContext,
    ResolvedReadPlan,
    ResolvedWritePlan,
)
from dc43_service_clients.governance import normalise_pipeline_context
from dc43_service_clients.governance.lineage import OpenDataLineageEvent, encode_lineage_event


class _Assessment:
    def __init__(self, validation: ValidationResult) -> None:
        self.status = validation
        self.validation = validation
        self.draft = None


class _LineageGovernanceStub:
    def __init__(
        self,
        *,
        read_plan: ResolvedReadPlan | None = None,
        write_plan: ResolvedWritePlan | None = None,
    ) -> None:
        self._read_plan = read_plan
        self._write_plan = write_plan
        self.lineage_events: list[Mapping[str, object]] = []
        self.read_activities: list[tuple[ResolvedReadPlan, _Assessment]] = []
        self.write_activities: list[tuple[ResolvedWritePlan, _Assessment]] = []
        self.expectation_plan: list[Mapping[str, object]] = []
        self.reviewed_outcomes: list[
            tuple[ValidationResult, object, Mapping[str, object]]
        ] = []
        self.links: list[tuple[str, str | None, str, str]] = []

    def resolve_read_context(self, *, context: GovernanceReadContext) -> ResolvedReadPlan:  # type: ignore[override]
        assert self._read_plan is not None
        return self._read_plan

    def resolve_write_context(self, *, context: GovernanceWriteContext) -> ResolvedWritePlan:  # type: ignore[override]
        assert self._write_plan is not None
        return self._write_plan

    def describe_expectations(self, *, contract_id: str, contract_version: str):  # type: ignore[override]
        return list(self.expectation_plan)

    def evaluate_dataset(
        self,
        *,
        contract_id: str,
        contract_version: str,
        dataset_id: str,
        dataset_version: str,
        validation: ValidationResult | None,
        observations,
        pipeline_context,
        operation: str,
        bump: str = "minor",
        draft_on_violation: bool = False,
    ) -> _Assessment:  # type: ignore[override]
        payload = observations() if callable(observations) else observations
        if validation is None:
            validation = ValidationResult(ok=True, errors=[], warnings=[], metrics={}, schema={}, status="ok")
        elif validation.status == "unknown":
            validation.status = "ok"
        assessment = _Assessment(validation)
        assessment.payload = payload  # type: ignore[attr-defined]
        return assessment

    def evaluate_write_plan(
        self,
        *,
        plan: ResolvedWritePlan,
        validation: ValidationResult | None,
        observations,
    ) -> _Assessment:  # type: ignore[override]
        payload = observations() if callable(observations) else observations
        if validation is None:
            validation = ValidationResult(ok=True, errors=[], warnings=[], metrics={}, schema={}, status="ok")
        elif validation.status == "unknown":
            validation.status = "ok"
        assessment = _Assessment(validation)
        assessment.payload = payload  # type: ignore[attr-defined]
        return assessment

    def register_read_activity(self, *, plan: ResolvedReadPlan, assessment: _Assessment) -> None:  # type: ignore[override]
        self.read_activities.append((plan, assessment))

    def register_write_activity(self, *, plan: ResolvedWritePlan, assessment: _Assessment) -> None:  # type: ignore[override]
        self.write_activities.append((plan, assessment))

    def publish_lineage_event(self, *, event: OpenDataLineageEvent) -> None:  # type: ignore[override]
        self.lineage_events.append(encode_lineage_event(event))

    def review_validation_outcome(  # type: ignore[override]
        self,
        *,
        validation: ValidationResult,
        base_contract,
        **kwargs,
    ):
        self.reviewed_outcomes.append((validation, base_contract, dict(kwargs)))
        return base_contract

    def link_dataset_contract(
        self,
        *,
        dataset_id: str,
        dataset_version: str | None,
        contract_id: str,
        contract_version: str,
    ) -> None:
        self.links.append((dataset_id, dataset_version, contract_id, contract_version))


def test_build_lineage_event_includes_facets(tmp_path: Path) -> None:
    contract = build_orders_contract(tmp_path / "orders")
    binding = DataProductInputBinding(
        data_product="Sales.Orders",
        port_name="orders.raw",
        data_product_version="1.0.0",
    )
    plan = ResolvedReadPlan(
        contract=contract,
        contract_id=contract.id,
        contract_version=contract.version,
        dataset_id="orders",
        dataset_version="2024-01-01",
        dataset_format="parquet",
        input_binding=binding,
        pipeline_context={"job_name": "orders-read"},
        bump="minor",
        draft_on_violation=False,
    )
    validation = ValidationResult(
        ok=True,
        status="ok",
        errors=[],
        warnings=[],
        metrics={"rowCount": 2},
        schema={"fields": {"order_id": "bigint"}},
    )

    event = build_lineage_run_event(
        operation="read",
        plan=plan,
        pipeline_context={"run_id": "run-001"},
        contract_id=contract.id,
        contract_version=contract.version,
        dataset_id="orders",
        dataset_version="2024-01-01",
        dataset_format="delta",
        table="analytics.orders",
        path="/mnt/orders",
        binding=binding,
        validation=validation,
        expectation_plan=[{"key": "rule", "predicate": "amount > 0"}],
    )

    payload = encode_lineage_event(event)
    assert payload["eventType"] == "COMPLETE"
    assert payload["job"]["name"] == "orders-read"
    dataset = payload["inputs"][0]
    facets = dataset["facets"]
    assert facets["dc43Contract"]["contractId"] == contract.id
    assert facets["dc43DataProduct"]["dataProduct"] == "Sales.Orders"
    assert facets["dc43DataQuality"]["metrics"]["rowCount"] == 2
    assert payload["run"]["facets"]["dc43Validation"]["status"] == "ok"


def test_lineage_emitted_for_governed_read(
    spark,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    dataset_path = materialise_orders(spark, tmp_path / "orders")
    contract = build_orders_contract(dataset_path)
    binding = DataProductInputBinding(
        data_product="Sales.Orders",
        port_name="orders.raw",
    )
    plan = ResolvedReadPlan(
        contract=contract,
        contract_id=contract.id,
        contract_version=contract.version,
        dataset_id="test.orders",
        dataset_version="2024-01-01",
        dataset_format="parquet",
        input_binding=binding,
        pipeline_context={"job_name": "orders-read"},
        bump="minor",
        draft_on_violation=False,
    )
    governance = _LineageGovernanceStub(read_plan=plan)

    monkeypatch.setenv("DC43_GOVERNANCE_PUBLICATION_MODE", "open_data_lineage")
    request = GovernanceSparkReadRequest(
        context=GovernanceReadContext(contract={"contract_id": contract.id, "contract_version": contract.version}),
        format="parquet",
        path=str(dataset_path),
    )
    request.context.pipeline_context = normalise_pipeline_context({"run_id": "read-run"})

    df, status = read_with_governance(
        spark,
        request,
        governance_service=governance,
        enforce=True,
        auto_cast=True,
        return_status=True,
    )

    assert df.count() == 2
    assert status is not None
    assert governance.lineage_events, "governance client should receive lineage"
    assert not governance.read_activities, "register_read_activity should be skipped in open data lineage mode"
    event = governance.lineage_events[-1]
    assert event["inputs"][0]["facets"]["dc43Contract"]["contractId"] == contract.id
    assert event["run"]["facets"]["dc43Validation"]["status"] in {"ok", None}


def test_governed_read_registers_activity_by_default(spark, tmp_path: Path) -> None:
    dataset_path = materialise_orders(spark, tmp_path / "orders-default")
    contract = build_orders_contract(dataset_path)
    plan = ResolvedReadPlan(
        contract=contract,
        contract_id=contract.id,
        contract_version=contract.version,
        dataset_id="test.orders.default",
        dataset_version="2024-01-01",
        dataset_format="parquet",
        pipeline_context={"job_name": "orders-read-default"},
        bump="minor",
        draft_on_violation=False,
    )
    governance = _LineageGovernanceStub(read_plan=plan)

    request = GovernanceSparkReadRequest(
        context=GovernanceReadContext(contract={"contract_id": contract.id, "contract_version": contract.version}),
        format="parquet",
        path=str(dataset_path),
    )

    read_with_governance(
        spark,
        request,
        governance_service=governance,
        enforce=True,
        auto_cast=True,
        return_status=False,
    )

    assert governance.read_activities, "register_read_activity should run when open data lineage is disabled"
    assert not governance.lineage_events, "lineage events should be disabled by default"


def test_lineage_emitted_for_governed_write(
    spark,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    output_path = tmp_path / "out"
    contract = build_orders_contract(output_path)
    binding = DataProductOutputBinding(
        data_product="Sales.Orders",
        port_name="orders.curated",
    )
    plan = ResolvedWritePlan(
        contract=contract,
        contract_id=contract.id,
        contract_version=contract.version,
        dataset_id="test.orders.curated",
        dataset_version="2024-01-01",
        dataset_format="parquet",
        output_binding=binding,
        pipeline_context={"job_name": "orders-write"},
        bump="minor",
        draft_on_violation=False,
    )
    governance = _LineageGovernanceStub(write_plan=plan)

    df = spark.createDataFrame([(1, "EUR"), (2, "USD")], ["order_id", "currency"])
    monkeypatch.setenv("DC43_GOVERNANCE_PUBLICATION_MODE", "open_data_lineage")
    request = GovernanceSparkWriteRequest(
        context=GovernanceWriteContext(contract={"contract_id": contract.id, "contract_version": contract.version}),
        format="parquet",
        path=str(output_path),
    )
    request.context.pipeline_context = normalise_pipeline_context({"run_id": "write-run"})

    result, status = write_with_governance(
        df=df,
        request=request,
        governance_service=governance,
        enforce=True,
        auto_cast=True,
        return_status=True,
    )

    assert result.ok
    assert status is None or status.ok
    assert governance.lineage_events, "governance client should receive lineage"
    assert not governance.write_activities, "register_write_activity should be skipped in open data lineage mode"
    event = governance.lineage_events[-1]
    assert event["outputs"][0]["facets"]["dc43DataProduct"]["dataProduct"] == "Sales.Orders"
    assert event["run"]["facets"]["dc43Validation"]["ok"]


def test_governed_write_registers_activity_by_default(spark, tmp_path: Path) -> None:
    output_path = tmp_path / "out-default"
    contract = build_orders_contract(output_path)
    plan = ResolvedWritePlan(
        contract=contract,
        contract_id=contract.id,
        contract_version=contract.version,
        dataset_id="test.orders.default.write",
        dataset_version="2024-01-01",
        dataset_format="parquet",
        pipeline_context={"job_name": "orders-write-default"},
        bump="minor",
        draft_on_violation=False,
    )
    governance = _LineageGovernanceStub(write_plan=plan)

    df = spark.createDataFrame([(1, "EUR"), (2, "USD")], ["order_id", "currency"])
    request = GovernanceSparkWriteRequest(
        context=GovernanceWriteContext(contract={"contract_id": contract.id, "contract_version": contract.version}),
        format="parquet",
        path=str(output_path),
    )

    write_with_governance(
        df=df,
        request=request,
        governance_service=governance,
        enforce=True,
        auto_cast=True,
        return_status=False,
    )

    assert governance.write_activities, "register_write_activity should run when open data lineage is disabled"
    assert not governance.lineage_events, "lineage events should not emit by default"
