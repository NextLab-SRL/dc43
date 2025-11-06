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
    read_from_contract,
    read_stream_with_contract,
    write_stream_with_contract,
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
            ValidationResult(ok=True, status="ok"),
            validation=validation,
            draft=None,
        )

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
    dq = RecordingDQService()
    locator = StaticDatasetLocator(format="rate")

    df, status = read_stream_with_contract(
        spark=spark,
        contract_id=contract.id,
        contract_service=service,
        expected_contract_version=f"=={contract.version}",
        data_quality_service=dq,
        dataset_locator=locator,
        options={"rowsPerSecond": "1"},
    )

    assert df.isStreaming
    assert status is None
    assert df.sparkSession is spark
    assert df.columns == ["timestamp", "value"]
    assert len(dq.describe_contracts) == 1
    assert len(dq.payloads) == 1
    payload = dq.payloads[0]
    assert payload.metrics == {}
    assert set(payload.schema) == {"timestamp", "value"}
    assert payload.schema["timestamp"]["odcs_type"] == "timestamp"
    assert payload.schema["value"]["odcs_type"] == "bigint"


def test_streaming_read_surfaces_dataset_version(spark, tmp_path: Path) -> None:
    contract, service = _stream_contract(tmp_path)
    dq = RecordingDQService()
    governance = RecordingGovernanceService(
        contracts={(contract.id, contract.version): contract},
        contract_service=service,
    )
    locator = StaticDatasetLocator(format="rate")

    df, status = read_stream_with_contract(
        spark=spark,
        contract_id=contract.id,
        contract_service=service,
        expected_contract_version=f"=={contract.version}",
        data_quality_service=dq,
        governance_service=governance,
        dataset_locator=locator,
        options={"rowsPerSecond": "1"},
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
    validation = call["validation"]
    assert isinstance(validation, ValidationResult)
    assert validation.details.get("dataset_version") == details["dataset_version"]


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
    dq = RecordingDQService()
    locator = StaticDatasetLocator(format="memory")
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

    result = write_stream_with_contract(
        df=df,
        contract_id=contract.id,
        contract_service=service,
        expected_contract_version=f"=={contract.version}",
        data_quality_service=dq,
        dataset_locator=locator,
        format="memory",
        options={"queryName": "stream_sink"},
        governance_service=governance,
        on_streaming_batch=_record,
    )

    assert result.ok
    assert len(dq.describe_contracts) == 1
    assert len(dq.payloads) >= 1
    queries = result.details.get("streaming_queries") or []
    assert len(queries) == 2
    deadline = time.time() + 10
    batch_payload: ObservationPayload | None = None
    while time.time() < deadline:
        for handle in queries:
            handle.processAllAvailable()
        if len(dq.payloads) >= 2:
            candidate = dq.payloads[-1]
            if candidate.metrics.get("row_count", 0) > 0:
                batch_payload = candidate
                break
        time.sleep(0.2)

    for handle in queries:
        handle.stop()

    assert len(dq.payloads) >= 2
    assert batch_payload is not None
    assert batch_payload.metrics
    assert batch_payload.metrics.get("row_count", 0) > 0

    assert result.details.get("dataset_id") == contract.id
    assert result.details.get("dataset_version")
    assert result.details.get("dataset_version") != "unknown"
    streaming_metrics = result.details.get("streaming_metrics") or {}
    assert streaming_metrics.get("row_count", 0) > 0
    batches = result.details.get("streaming_batches") or []
    assert batches
    assert any((batch.get("row_count", 0) or 0) > 0 for batch in batches)
    assert governance.evaluate_calls
    write_call = governance.evaluate_calls[0]
    assert write_call["dataset_version"] == result.details["dataset_version"]
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
    dq = ControlledDQService(fail_after=3)
    locator = StaticDatasetLocator(format="memory")
    governance = RecordingGovernanceService(
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

    result = write_stream_with_contract(
        df=df,
        contract_id=contract.id,
        contract_service=service,
        expected_contract_version=f"=={contract.version}",
        data_quality_service=dq,
        dataset_locator=locator,
        format="memory",
        options={"queryName": "intervention_sink"},
        governance_service=governance,
        enforce=False,
        streaming_intervention_strategy=BlockOnFailure(),
    )

    queries = result.details.get("streaming_queries") or []
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
        captured = result.details.get("streaming_intervention_reason")
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

    assert len(dq.payloads) >= 3
    assert result.details.get("streaming_metrics")
    batches = result.details.get("streaming_batches") or []
    assert batches
    assert any(batch.get("intervention") for batch in batches)


def test_streaming_enforcement_stops_sink_on_failure(spark, tmp_path: Path) -> None:
    contract, service = _stream_contract(tmp_path)
    dq = ControlledDQService(fail_after=2)
    locator = StaticDatasetLocator(format="memory")
    governance = RecordingGovernanceService(
        contracts={(contract.id, contract.version): contract},
        contract_service=service,
    )

    df = (
        spark.readStream.format("rate")
        .options(rowsPerSecond="5", numPartitions="1")
        .load()
    )

    result = write_stream_with_contract(
        df=df,
        contract_id=contract.id,
        contract_service=service,
        expected_contract_version=f"=={contract.version}",
        data_quality_service=dq,
        dataset_locator=locator,
        format="memory",
        options={"queryName": "enforced_sink"},
        governance_service=governance,
        enforce=True,
    )

    queries = result.details.get("streaming_queries") or []
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
