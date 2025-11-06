from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace
from typing import Mapping

import pytest

from dc43_integrations.spark.io import (
    GovernanceSparkReadRequest,
    GovernanceSparkWriteRequest,
    read_with_governance,
    write_with_governance,
)
from dc43_integrations.spark import open_telemetry
from dc43_service_clients.data_products import DataProductInputBinding, DataProductOutputBinding
from dc43_service_clients.data_quality import ValidationResult
from dc43_service_clients.governance import normalise_pipeline_context
from dc43_service_clients.governance.models import (
    GovernanceReadContext,
    GovernanceWriteContext,
    ResolvedReadPlan,
    ResolvedWritePlan,
)

from .helpers.orders import build_orders_contract, materialise_orders


class _RecordingSpan:
    def __init__(self, name: str, kind: str | None) -> None:
        self.name = name
        self.kind = kind
        self.attributes: dict[str, object] = {}
        self.events: list[tuple[str, Mapping[str, object]]] = []

    def __enter__(self) -> "_RecordingSpan":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:  # pragma: no cover - required by context protocol
        return None

    def set_attribute(self, key: str, value) -> None:
        self.attributes[key] = value

    def add_event(self, name: str, attributes: Mapping[str, object] | None = None) -> None:
        payload = dict(attributes or {})
        self.events.append((name, payload))


class _RecordingTracer:
    def __init__(self) -> None:
        self.spans: list[_RecordingSpan] = []

    def start_as_current_span(self, name: str, *, kind=None) -> _RecordingSpan:
        span = _RecordingSpan(name, kind)
        self.spans.append(span)
        return span


class _Resolver:
    def __init__(self, tracer: _RecordingTracer) -> None:
        self._tracer = tracer
        self._span_kind = SimpleNamespace(INTERNAL="internal")

    def __call__(self) -> tuple[_RecordingTracer, SimpleNamespace]:
        return self._tracer, self._span_kind

    def cache_clear(self) -> None:  # pragma: no cover - compatibility shim
        return None


class _Assessment:
    def __init__(self, validation: ValidationResult) -> None:
        self.status = validation
        self.validation = validation
        self.draft = None


class _TelemetryGovernanceStub:
    def __init__(
        self,
        *,
        read_plan: ResolvedReadPlan | None = None,
        write_plan: ResolvedWritePlan | None = None,
    ) -> None:
        self._read_plan = read_plan
        self._write_plan = write_plan
        self.read_activities: list[tuple[ResolvedReadPlan, _Assessment]] = []
        self.write_activities: list[tuple[ResolvedWritePlan, _Assessment]] = []
        self.expectation_plan: list[Mapping[str, object]] = []
        self.linked_contracts: list[Mapping[str, str]] = []

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
            validation = ValidationResult(
                ok=True,
                status="ok",
                errors=[],
                warnings=[],
                metrics={},
                schema={},
            )
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
            validation = ValidationResult(
                ok=True,
                status="ok",
                errors=[],
                warnings=[],
                metrics={},
                schema={},
            )
        elif validation.status == "unknown":
            validation.status = "ok"
        assessment = _Assessment(validation)
        assessment.payload = payload  # type: ignore[attr-defined]
        return assessment

    def register_read_activity(self, *, plan: ResolvedReadPlan, assessment: _Assessment) -> None:  # type: ignore[override]
        self.read_activities.append((plan, assessment))

    def register_write_activity(self, *, plan: ResolvedWritePlan, assessment: _Assessment) -> None:  # type: ignore[override]
        self.write_activities.append((plan, assessment))

    def link_dataset_contract(
        self,
        *,
        dataset_id: str,
        dataset_version: str,
        contract_id: str,
        contract_version: str,
    ) -> None:  # type: ignore[override]
        self.linked_contracts.append(
            {
                "dataset_id": dataset_id,
                "dataset_version": dataset_version,
                "contract_id": contract_id,
                "contract_version": contract_version,
            }
        )


@pytest.fixture
def _patched_tracer(monkeypatch: pytest.MonkeyPatch) -> _RecordingTracer:
    tracer = _RecordingTracer()
    resolver = _Resolver(tracer)
    open_telemetry._resolve_tracer.cache_clear()
    monkeypatch.setattr(open_telemetry, "_resolve_tracer", resolver)
    return tracer


def test_open_telemetry_read_emits_span(
    spark,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    _patched_tracer: _RecordingTracer,
) -> None:
    tracer = _patched_tracer
    dataset_path = materialise_orders(spark, tmp_path / "otel-orders")
    contract = build_orders_contract(dataset_path)
    binding = DataProductInputBinding(
        data_product="Sales.Orders",
        port_name="orders.raw",
        data_product_version="1.0.0",
    )
    plan = ResolvedReadPlan(
        contract=contract,
        contract_id=contract.id,
        contract_version=contract.version,
        dataset_id="otel.orders",
        dataset_version="2024-01-01",
        dataset_format="parquet",
        input_binding=binding,
        pipeline_context={"job_name": "orders-read-otel"},
        bump="minor",
        draft_on_violation=False,
    )
    governance = _TelemetryGovernanceStub(read_plan=plan)

    monkeypatch.setenv("DC43_GOVERNANCE_PUBLICATION_MODE", "open_telemetry")
    request = GovernanceSparkReadRequest(
        context=GovernanceReadContext(contract={"contract_id": contract.id, "contract_version": contract.version}),
        format="parquet",
        path=str(dataset_path),
    )
    request.context.pipeline_context = normalise_pipeline_context({"run_id": "otel-read"})

    df, status = read_with_governance(
        spark,
        request,
        governance_service=governance,
        enforce=True,
        auto_cast=True,
        return_status=True,
    )

    assert df.count() == 2
    assert status is not None and status.ok
    assert not governance.read_activities, "register_read_activity should be skipped in open telemetry mode"
    assert tracer.spans, "telemetry span should be recorded"
    span = tracer.spans[-1]
    assert span.name.endswith(".read")
    assert span.attributes["dc43.governance.operation"] == "read"
    assert span.attributes["dc43.governance.dataset.id"] == "otel.orders"
    assert span.attributes["dc43.governance.pipeline.run_id"] == "otel-read"
    assert any(name == "dc43.validation" for name, _ in span.events)


def test_open_telemetry_write_emits_span(
    spark,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    _patched_tracer: _RecordingTracer,
) -> None:
    tracer = _patched_tracer
    output_path = tmp_path / "otel-out"
    contract = build_orders_contract(output_path)
    binding = DataProductOutputBinding(
        data_product="Sales.Orders",
        port_name="orders.curated",
    )
    plan = ResolvedWritePlan(
        contract=contract,
        contract_id=contract.id,
        contract_version=contract.version,
        dataset_id="otel.orders.curated",
        dataset_version="2024-01-02",
        dataset_format="parquet",
        output_binding=binding,
        pipeline_context={"job_name": "orders-write-otel"},
        bump="minor",
        draft_on_violation=False,
    )
    governance = _TelemetryGovernanceStub(write_plan=plan)

    df = spark.createDataFrame([(1, "EUR"), (2, "USD")], ["order_id", "currency"])
    monkeypatch.setenv("DC43_GOVERNANCE_PUBLICATION_MODE", "open_telemetry")
    request = GovernanceSparkWriteRequest(
        context=GovernanceWriteContext(contract={"contract_id": contract.id, "contract_version": contract.version}),
        format="parquet",
        path=str(output_path),
    )
    request.context.pipeline_context = normalise_pipeline_context({"run_id": "otel-write"})

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
    assert not governance.write_activities, "register_write_activity should be skipped in open telemetry mode"
    assert tracer.spans, "telemetry span should be recorded"
    span = tracer.spans[-1]
    assert span.name.endswith(".write")
    assert span.attributes["dc43.governance.operation"] == "write"
    assert span.attributes["dc43.governance.dataset.id"] == "otel.orders.curated"
    assert span.attributes["dc43.governance.pipeline.run_id"] == "otel-write"
    assert any(name == "dc43.validation" for name, _ in span.events)
