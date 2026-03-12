from __future__ import annotations

from pathlib import Path
import time

from typing import Mapping

import pytest
pytest.importorskip(
    "openlineage.client.run", reason="openlineage-python is required for lineage streaming tests"
)
from pyspark.sql.utils import StreamingQueryException

from open_data_contract_standard.model import (  # type: ignore
    Description,
    OpenDataContractStandard,
    SchemaObject,
    SchemaProperty,
    Server,
)

from dc43_service_backends.contracts.backend.stores import FSContractStore
from dc43_service_clients.contracts import LocalContractServiceClient
from dc43_service_clients.data_quality import ObservationPayload, ValidationResult
from dc43_service_clients.governance.lineage import OpenDataLineageEvent, encode_lineage_event
from dc43_integrations.spark.io import (
    StaticDatasetLocator,
    StreamingInterventionContext,
    StreamingInterventionError,
    StreamingInterventionStrategy,
    read_with_governance,
    read_stream_with_governance,
    write_with_governance,
    GovernanceSparkReadRequest,
    GovernanceSparkWriteRequest,
)
from dc43_service_clients.governance.models import (
    ContractReference,
    GovernanceReadContext,
    GovernanceWriteContext,
)


class RecordingDQService:
    """Data quality stub that records evaluation calls."""

    def __init__(self) -> None:
        self.describe_contracts: list[OpenDataContractStandard] = []
        self.payloads: list[ObservationPayload] = []

    def describe_expectations(self, *, contract: OpenDataContractStandard):  # type: ignore[override]
        self.describe_contracts.append(contract)
        return []

    def evaluate(self, *, contract: OpenDataContractStandard, payload):  # type: ignore[override]
        self.payloads.append(payload)
        return ValidationResult(ok=True, errors=[], warnings=[], metrics=payload.metrics)


class ControlledDQService(RecordingDQService):
    """DQ stub that flips to failures after a configurable number of calls."""

    def __init__(self, *, fail_after: int) -> None:
        super().__init__()
        self._fail_after = fail_after
        self._calls = 0

    def evaluate(self, *, contract: OpenDataContractStandard, payload):  # type: ignore[override]
        self.payloads.append(payload)
        self._calls += 1
        if self._calls >= self._fail_after:
            return ValidationResult(
                ok=False,
                errors=[f"failed batch {self._calls}"],
                warnings=[],
                metrics=payload.metrics,
            )
        return ValidationResult(ok=True, errors=[], warnings=[], metrics=payload.metrics)


class RecordingGovernanceService:
    """Governance stub that records dataset evaluations and serves contract lookups."""

    class Assessment:
        def __init__(
            self,
            status: ValidationResult,
            *,
            validation: ValidationResult | None = None,
            draft: object | None = None,
        ) -> None:
            self.status = status
            self.validation = validation
            self.draft = draft

    def __init__(
        self,
        *,
        contracts: Mapping[tuple[str, str], OpenDataContractStandard] | None = None,
        contract_service: LocalContractServiceClient | None = None,
    ) -> None:
        self.evaluate_calls: list[dict[str, object]] = []
        self.review_calls: list[dict[str, object]] = []
        self.link_calls: list[dict[str, object]] = []
        self.lineage_calls: list[Mapping[str, object]] = []
        self._contracts: dict[tuple[str, str], OpenDataContractStandard] = {}
        if contracts:
            self._contracts.update(contracts)
        self._contract_service = contract_service

    def _register_contract(self, contract: OpenDataContractStandard) -> None:
        self._contracts[(contract.id, contract.version)] = contract

    def evaluate_dataset(
        self,
        *,
        contract_id: str,
        contract_version: str,
        dataset_id: str,
        dataset_version: str,
        validation: ValidationResult,
        observations,
        pipeline_context,
        operation: str,
        bump: str = "minor",
        draft_on_violation: bool = False,
    ) -> "RecordingGovernanceService.Assessment":  # type: ignore[override]
        payload = observations() if callable(observations) else observations
        self.evaluate_calls.append(
            {
                "contract_id": contract_id,
                "contract_version": contract_version,
                "dataset_id": dataset_id,
                "dataset_version": dataset_version,
                "validation": validation,
                "payload": payload,
                "operation": operation,
            }
        )
        return RecordingGovernanceService.Assessment(
            ValidationResult(
                ok=True,
                status="ok",
                metrics=payload.metrics if payload and hasattr(payload, "metrics") else {},
                details={
                    "dataset_id": dataset_id,
                    "dataset_version": dataset_version,
                },
            ),
            validation=validation,
            draft=None,
        )

    def register_read_activity(self, *, plan, assessment=None):
        self.evaluate_calls.append(
            {
                "operation": "read",
                "plan": plan,
                "assessment": assessment,
            }
        )

    def resolve_read_context(self, *, context: GovernanceReadContext):
        from dc43_service_clients.governance.models import ResolvedReadPlan, GovernancePolicy
        contract = None
        if context.contract:
            contract = self.get_contract(contract_id=context.contract.contract_id, contract_version=context.contract.contract_version)
        elif context.input_binding:
            contract = self.get_contract(contract_id="demo.rate_stream", contract_version="0.1.0")
        
        return ResolvedReadPlan(
            contract=contract,
            contract_id=contract.id if contract else "demo.rate_stream",
            contract_version=contract.version if contract else "0.1.0",
            dataset_id=context.dataset_id or (contract.id if contract else "dataset"),
            dataset_version=context.dataset_version or "2024-01-01",
            dataset_format=context.dataset_format,
            input_binding=context.input_binding,
            pipeline_context=context.pipeline_context,
            policy=context.policy or GovernancePolicy(),
        )

    def resolve_write_context(self, *, context: GovernanceWriteContext):
        from dc43_service_clients.governance.models import ResolvedWritePlan, GovernancePolicy
        contract = None
        if context.contract:
            contract = self.get_contract(contract_id=context.contract.contract_id, contract_version=context.contract.contract_version)
        elif context.output_binding:
            contract = self.get_contract(contract_id="demo.rate_stream", contract_version="0.1.0")

        return ResolvedWritePlan(
            contract=contract,
            contract_id=contract.id if contract else "demo.rate_stream",
            contract_version=contract.version if contract else "0.1.0",
            dataset_id=context.dataset_id or "dataset",
            dataset_version=context.dataset_version or "2024-01-01",
            dataset_format=context.dataset_format,
            output_binding=context.output_binding,
            pipeline_context=context.pipeline_context,
            policy=context.policy or GovernancePolicy(),
        )

    def describe_expectations(self, *, contract_id: str, contract_version: str):
        return []

    def evaluate_read_plan(self, *, plan, validation=None, observations=None):
        return self.evaluate_dataset(
            contract_id=plan.contract_id,
            contract_version=plan.contract_version,
            dataset_id=plan.dataset_id,
            dataset_version=plan.dataset_version,
            validation=validation,
            observations=observations,
            pipeline_context=plan.pipeline_context,
            operation="read",
        )

    def evaluate_write_plan(self, *, plan, validation=None, observations=None):
        return self.evaluate_dataset(
            contract_id=plan.contract_id,
            contract_version=plan.contract_version,
            dataset_id=plan.dataset_id,
            dataset_version=plan.dataset_version,
            validation=validation,
            observations=observations,
            pipeline_context=plan.pipeline_context,
            operation="write",
        )

    def register_write_activity(self, *, plan, assessment):
        pass

    def review_validation_outcome(self, **kwargs):  # type: ignore[override]
        self.review_calls.append(kwargs)
        return None

    def link_dataset_contract(self, **kwargs) -> None:  # type: ignore[override]
        self.link_calls.append(kwargs)

    def get_contract(self, *, contract_id: str, contract_version: str) -> OpenDataContractStandard:
        contract = self._contracts.get((contract_id, contract_version))
        if contract is not None:
            return contract
        if self._contract_service is not None:
            contract = self._contract_service.get(contract_id, contract_version)
            self._register_contract(contract)
            return contract
        raise ValueError(f"Unknown contract {contract_id}:{contract_version}")

    def latest_contract(self, *, contract_id: str) -> OpenDataContractStandard | None:
        versions = [
            contract
            for (cid, _), contract in self._contracts.items()
            if cid == contract_id
        ]
        if versions:
            # Return the lexicographically greatest version for determinism.
            return sorted(versions, key=lambda item: item.version)[-1]
        if self._contract_service is not None:
            contract = self._contract_service.latest(contract_id)
            if contract is not None:
                self._register_contract(contract)
            return contract
        return None

    def list_contract_versions(self, *, contract_id: str) -> list[str]:
        versions = [version for (cid, version) in self._contracts if cid == contract_id]
        if versions:
            return sorted(versions)
        if self._contract_service is not None:
            return list(self._contract_service.list_versions(contract_id))
        return []

    def publish_lineage_event(self, *, event: OpenDataLineageEvent) -> None:  # type: ignore[override]
        self.lineage_calls.append(encode_lineage_event(event))


def _stream_contract(tmp_path: Path) -> tuple[OpenDataContractStandard, LocalContractServiceClient]:
    contract = OpenDataContractStandard(
        version="0.1.0",
        kind="DataContract",
        apiVersion="3.0.2",
        id="demo.rate_stream",
        name="Rate stream",
        description=Description(usage="Streaming rate source"),
        schema=[
            SchemaObject(
                name="rate",
                properties=[
                    SchemaProperty(name="timestamp", physicalType="timestamp", required=True),
                    SchemaProperty(name="value", physicalType="bigint", required=True),
                ],
            )
        ],
        servers=[Server(server="local", type="stream", format="rate")],
    )
    store = FSContractStore(str(tmp_path / "contracts"))
    store.put(contract)
    return contract, LocalContractServiceClient(store)


def test_streaming_read_invokes_dq_without_metrics(spark, tmp_path: Path) -> None:
    contract, service = _stream_contract(tmp_path)
    governance = RecordingGovernanceService(
        contracts={(contract.id, contract.version): contract},
        contract_service=service,
    )
    locator = StaticDatasetLocator(format="rate")

    df, status = read_stream_with_governance(
        spark=spark,
        request=GovernanceSparkReadRequest(
            context=GovernanceReadContext(
                contract=ContractReference(
                    contract_id=contract.id,
                    contract_version=contract.version,
                )
            ),
            dataset_locator=locator,
            options={"rowsPerSecond": "1"},
        ),
        governance_service=governance,
        return_status=True,
    )

    print(f"DEBUG: df type is {type(df)}")
    if hasattr(df, "columns"):
        print(f"DEBUG: df columns are {df.columns}")
    assert df.isStreaming
    assert status is not None
    assert status.ok
    assert df.sparkSession is spark
    assert df.columns == ["timestamp", "value"]
    assert len(governance.evaluate_calls) == 2
    call = governance.evaluate_calls[0]
    payload = call["payload"]
    assert payload.metrics == {}
    assert set(payload.schema) == {"timestamp", "value"}
    assert payload.schema["timestamp"]["odcs_type"] == "timestamp"
    assert payload.schema["value"]["odcs_type"] == "bigint"


def test_streaming_read_surfaces_dataset_version(spark, tmp_path: Path) -> None:
    contract, service = _stream_contract(tmp_path)
    governance = RecordingGovernanceService(
        contracts={(contract.id, contract.version): contract},
        contract_service=service,
    )
    locator = StaticDatasetLocator(format="rate")
    df, status = read_stream_with_governance(
        spark=spark,
        request=GovernanceSparkReadRequest(
            context=GovernanceReadContext(
                contract=ContractReference(
                    contract_id=contract.id,
                    contract_version=contract.version,
                )
            ),
            dataset_locator=locator,
            options={"rowsPerSecond": "1"},
        ),
        governance_service=governance,
        return_status=True,
    )

    assert df.isStreaming
    assert status is not None
    details = status.details
    assert details["dataset_id"] == contract.id
    assert details["dataset_version"]
    assert details["dataset_version"] != "unknown"
    assert governance.evaluate_calls
    call = governance.evaluate_calls[0]
    assert call["dataset_version"] == details["dataset_version"]
    # validation was removed from the call in favor of return status
    assert isinstance(status, ValidationResult)
    assert status.details.get("dataset_version") == details["dataset_version"]


def test_streaming_write_returns_query_and_validation(spark, tmp_path: Path) -> None:
    contract = OpenDataContractStandard(
        version="0.1.0",
        kind="DataContract",
        apiVersion="3.0.2",
        id="demo.rate_sink",
        name="Rate sink",
        description=Description(usage="Streaming rate sink"),
        schema=[
            SchemaObject(
                name="rate",
                properties=[
                    SchemaProperty(name="timestamp", physicalType="timestamp", required=True),
                    SchemaProperty(name="value", physicalType="bigint", required=True),
                ],
            )
        ],
        servers=[Server(server="memory", type="stream", format="memory")],
    )
    store = FSContractStore(str(tmp_path / "contracts"))
    store.put(contract)
    service = LocalContractServiceClient(store)
    locator = StaticDatasetLocator(format="memory", dataset_version="2024-01-01")
    governance = RecordingGovernanceService(
        contracts={(contract.id, contract.version): contract},
        contract_service=service,
    )

    df = (
        spark.readStream.format("rate")
        .options(rowsPerSecond="5", numPartitions="1")
        .load()
    )

    events: list[dict[str, object]] = []

    def _record(event: Mapping[str, object]) -> None:
        events.append(dict(event))

    result = write_with_governance(
        df=df,
        request=GovernanceSparkWriteRequest(
            context=GovernanceWriteContext(
                contract=ContractReference(
                    contract_id=contract.id,
                    contract_version=contract.version,
                )
            ),
            dataset_locator=locator,
            format="memory",
            options={"queryName": "stream_sink"},
        ),
        governance_service=governance,
        streaming_batch_callback=_record,
    )

    assert result.result.ok
    assert len(governance.evaluate_calls) >= 1
    queries = result.streaming_queries
    assert len(queries) == 2
    deadline = time.time() + 10
    batch_payload: ObservationPayload | None = None
    while time.time() < deadline:
        for handle in queries:
            handle.processAllAvailable()
        if len(governance.evaluate_calls) >= 2:
            candidate = governance.evaluate_calls[-1]["payload"]
            if candidate.metrics.get("row_count", 0) > 0:
                batch_payload = candidate
                break
        time.sleep(0.2)

    for handle in queries:
        handle.stop()

    assert len(governance.evaluate_calls) >= 2
    assert batch_payload is not None
    assert batch_payload.metrics
    assert batch_payload.metrics.get("row_count", 0) > 0

    assert result.result.details.get("dataset_id") == contract.id
    assert result.result.details.get("dataset_version")
    assert result.result.details.get("dataset_version") != "unknown"
    streaming_metrics = result.result.details.get("streaming_metrics") or {}
    assert streaming_metrics.get("row_count", 0) > 0
    batches = result.result.details.get("streaming_batches") or []
    assert batches
    assert any((batch.get("row_count", 0) or 0) > 0 for batch in batches)
    assert governance.evaluate_calls
    write_call = governance.evaluate_calls[0]
    assert write_call["dataset_version"] == result.result.details["dataset_version"]
    assert events, "expected streaming callback events"
    assert any(event.get("type") == "batch" for event in events)
    assert any((event.get("row_count", 0) or 0) > 0 for event in events if event.get("type") == "batch")


def test_streaming_intervention_blocks_after_failure(spark, tmp_path: Path) -> None:
    contract = OpenDataContractStandard(
        version="0.1.0",
        kind="DataContract",
        apiVersion="3.0.2",
        id="demo.intervention_sink",
        name="Intervention sink",
        description=Description(usage="Streaming intervention"),
        schema=[
            SchemaObject(
                name="rate",
                properties=[
                    SchemaProperty(name="timestamp", physicalType="timestamp", required=True),
                    SchemaProperty(name="value", physicalType="bigint", required=True),
                ],
            )
        ],
        servers=[Server(server="memory", type="stream", format="memory")],
    )
    store = FSContractStore(str(tmp_path / "contracts"))
    store.put(contract)
    service = LocalContractServiceClient(store)
    class ControlledGovernanceService(RecordingGovernanceService):
        def __init__(self, *, fail_after: int, contracts, contract_service) -> None:
            super().__init__(contracts=contracts, contract_service=contract_service)
            self._fail_after = fail_after
            self._calls = 0

        def evaluate_dataset(self, **kwargs):
            payload = kwargs["observations"]() if callable(kwargs["observations"]) else kwargs["observations"]
            self.evaluate_calls.append(
                {
                    "contract_id": kwargs.get("contract_id"),
                    "contract_version": kwargs.get("contract_version"),
                    "dataset_id": kwargs.get("dataset_id"),
                    "dataset_version": kwargs.get("dataset_version"),
                    "validation": kwargs.get("validation"),
                    "payload": payload,
                    "operation": kwargs.get("operation"),
                }
            )
            self._calls += 1
            if self._calls >= self._fail_after:
                validation = ValidationResult(
                    ok=False,
                    errors=[f"failed batch {self._calls}"],
                    warnings=[],
                    metrics=payload.metrics,
                )
            else:
                validation = ValidationResult(ok=True, errors=[], warnings=[], metrics=payload.metrics)
            return RecordingGovernanceService.Assessment(
                ValidationResult(ok=validation.ok, status="ok" if validation.ok else "error"),
                validation=validation,
                draft=None,
            )

    locator = StaticDatasetLocator(format="memory", dataset_version="2024-01-01")
    governance = ControlledGovernanceService(
        fail_after=3,
        contracts={(contract.id, contract.version): contract},
        contract_service=service,
    )

    df = (
        spark.readStream.format("rate")
        .options(rowsPerSecond="5", numPartitions="1")
        .load()
    )

    class BlockOnFailure(StreamingInterventionStrategy):
        def decide(self, context: StreamingInterventionContext):
            if not context.validation.ok:
                return f"blocked batch {context.batch_id}"
            return None

    result = write_with_governance(
        df=df,
        request=GovernanceSparkWriteRequest(
            context=GovernanceWriteContext(
                contract=ContractReference(
                    contract_id=contract.id,
                    contract_version=contract.version,
                )
            ),
            dataset_locator=locator,
            format="memory",
            options={"queryName": "intervention_sink"},
            streaming_intervention_strategy=BlockOnFailure(),
        ),
        governance_service=governance,
        enforce=False,
    )

    queries = result.streaming_queries
    assert len(queries) == 2
    metrics_query = next(q for q in queries if "dc43_metrics" in (q.name or ""))
    sink_query = next(q for q in queries if q is not metrics_query)

    deadline = time.time() + 10
    reason: str | None = None
    while time.time() < deadline and reason is None:
        sink_query.processAllAvailable()
        try:
            metrics_query.processAllAvailable()
        except StreamingQueryException:
            pass
        captured = result.result.details.get("streaming_intervention_reason")
        if isinstance(captured, str):
            reason = captured
            break
        time.sleep(0.2)
    assert isinstance(reason, str)
    assert "blocked batch" in reason

    for handle in queries:
        try:
            handle.stop()
        except Exception:
            pass

    assert len(governance.evaluate_calls) >= 3
    assert result.result.details.get("streaming_metrics")
    batches = result.result.details.get("streaming_batches") or []
    assert batches
    assert any(batch.get("intervention") for batch in batches)


def test_streaming_enforcement_stops_sink_on_failure(spark, tmp_path: Path) -> None:
    contract, service = _stream_contract(tmp_path)
    class ControlledGovernanceService(RecordingGovernanceService):
        def __init__(self, *, fail_after: int, contracts, contract_service) -> None:
            super().__init__(contracts=contracts, contract_service=contract_service)
            self._fail_after = fail_after
            self._calls = 0

        def evaluate_dataset(self, **kwargs):
            payload = kwargs["observations"]() if callable(kwargs["observations"]) else kwargs["observations"]
            self.evaluate_calls.append(
                {
                    "contract_id": kwargs.get("contract_id"),
                    "contract_version": kwargs.get("contract_version"),
                    "dataset_id": kwargs.get("dataset_id"),
                    "dataset_version": kwargs.get("dataset_version"),
                    "validation": kwargs.get("validation"),
                    "payload": payload,
                    "operation": kwargs.get("operation"),
                }
            )
            self._calls += 1
            if self._calls >= self._fail_after:
                validation = ValidationResult(
                    ok=False,
                    errors=[f"failed batch {self._calls}"],
                    warnings=[],
                    metrics=payload.metrics,
                )
            else:
                validation = ValidationResult(ok=True, errors=[], warnings=[], metrics=payload.metrics)
            return RecordingGovernanceService.Assessment(
                ValidationResult(ok=validation.ok, status="ok" if validation.ok else "error"),
                validation=validation,
                draft=None,
            )

    locator = StaticDatasetLocator(format="memory", dataset_version="2024-01-01")
    governance = ControlledGovernanceService(
        fail_after=2,
        contracts={(contract.id, contract.version): contract},
        contract_service=service,
    )

    df = (
        spark.readStream.format("rate")
        .options(rowsPerSecond="5", numPartitions="1")
        .load()
    )

    result = write_with_governance(
        df=df,
        request=GovernanceSparkWriteRequest(
            context=GovernanceWriteContext(
                contract=ContractReference(
                    contract_id=contract.id,
                    contract_version=contract.version,
                )
            ),
            dataset_locator=locator,
            format="memory",
            options={"queryName": "enforced_sink"},
        ),
        governance_service=governance,
        enforce=True,
    )

    queries = result.streaming_queries
    assert len(queries) == 2
    metrics_query = next(q for q in queries if "dc43_metrics" in (q.name or ""))
    sink_query = next(q for q in queries if q is not metrics_query)

    failure_detected = False
    deadline = time.time() + 10
    while time.time() < deadline and not failure_detected:
        if sink_query.isActive:
            sink_query.processAllAvailable()
        try:
            metrics_query.processAllAvailable()
        except StreamingQueryException:
            failure_detected = True
            break
        time.sleep(0.2)

    assert failure_detected, "expected enforcement failure to surface"

    deadline = time.time() + 5
    while time.time() < deadline and sink_query.isActive:
        time.sleep(0.2)

    assert not sink_query.isActive, "streaming sink should stop after enforcement failure"
    assert not metrics_query.isActive

    if sink_query.isActive:
        sink_query.stop()
    if metrics_query.isActive:
        metrics_query.stop()

def test_streaming_dataset_version_resolves_templates(spark, tmp_path: Path) -> None:
    contract, service = _stream_contract(tmp_path)
    
    class InspectingGovernanceService(RecordingGovernanceService):
        def __init__(self, *, contracts, contract_service) -> None:
            super().__init__(contracts=contracts, contract_service=contract_service)
            self.dataset_versions_seen = []

        def evaluate_dataset(self, **kwargs):
            self.dataset_versions_seen.append(kwargs.get("dataset_version"))
            return RecordingGovernanceService.Assessment(
                ValidationResult(ok=True, status="ok"),
                validation=ValidationResult(ok=True, status="ok"),
                draft=None,
            )

        def evaluate_write_plan(self, **kwargs):
            plan = kwargs.get("plan")
            self.dataset_versions_seen.append(f"plan:{plan.dataset_version}")
            return RecordingGovernanceService.Assessment(
                ValidationResult(ok=True, status="ok"),
                validation=ValidationResult(ok=True, status="ok"),
                draft=None,
            )

    locator = StaticDatasetLocator(format="memory", dataset_version="{batch_id}")
    governance = InspectingGovernanceService(
        contracts={(contract.id, contract.version): contract},
        contract_service=service,
    )

    df = (
        spark.readStream.format("rate")
        .options(rowsPerSecond="5", numPartitions="1")
        .load()
    )

    result = write_with_governance(
        df=df,
        request=GovernanceSparkWriteRequest(
            context=GovernanceWriteContext(
                contract=ContractReference(
                    contract_id=contract.id,
                    contract_version=contract.version,
                )
            ),
            dataset_locator=locator,
            format="memory",
            options={"queryName": "version_template_test"},
        ),
        governance_service=governance,
        enforce=False,
    )

    queries = result.streaming_queries
    assert len(queries) == 2
    metrics_query = next(q for q in queries if "dc43_metrics" in (q.name or ""))
    sink_query = next(q for q in queries if q is not metrics_query)

    deadline = time.time() + 10
    while time.time() < deadline and len([v for v in governance.dataset_versions_seen if not str(v).startswith("plan:")]) < 2:
        if sink_query.isActive:
            sink_query.processAllAvailable()
        metrics_query.processAllAvailable()
        time.sleep(0.5)

    if sink_query.isActive:
        sink_query.stop()
    if metrics_query.isActive:
        metrics_query.stop()

    print("DATASET VERSIONS SEEN:", governance.dataset_versions_seen)
    assert "plan:init" in governance.dataset_versions_seen, "preflight evaluation should resolve '{batch_id}' to 'init' before streaming starts"
    batch_evals = [v for v in governance.dataset_versions_seen if str(v) != "plan:init"]
    assert len(batch_evals) >= 2, f"metrics loop should evaluate at least 2 distinct batches, got {batch_evals}"
    assert all(str(v).isdigit() for v in batch_evals), f"batch dataset_version templates should properly resolve to integers: {batch_evals}"


