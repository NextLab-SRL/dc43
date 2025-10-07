"""Scenario runner showcasing dc43 Spark streaming contract integrations."""
from __future__ import annotations

import argparse
import sys
import tempfile
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, List, Mapping, Optional, Sequence

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.streaming import StreamingQuery, StreamingQueryException

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

from dc43_integrations.spark.io import (
    StaticDatasetLocator,
    read_stream_with_contract,
    write_stream_with_contract,
)
from dc43.core.odcs import list_properties


class ScenarioDQService:
    """Data-quality stub that computes simple expectation outcomes."""

    def __init__(self, contract: OpenDataContractStandard) -> None:
        self.contract = contract
        self.payloads: List[ObservationPayload] = []
        self.describe_calls: List[OpenDataContractStandard] = []
        self.expectation_plan: Sequence[Mapping[str, object]] = [
            {
                "key": "non_negative_value",
                "rule": "predicate",
                "column": "value",
                "predicate": "value >= 0",
            },
            {
                "key": "value_not_null",
                "rule": "predicate",
                "column": "value",
                "predicate": "value IS NOT NULL",
            },
        ]
        self.expected_columns = {
            field.name
            for field in list_properties(contract)
            if getattr(field, "name", None)
        }

    def describe_expectations(self, *, contract: OpenDataContractStandard):  # type: ignore[override]
        self.describe_calls.append(contract)
        return list(self.expectation_plan)

    def evaluate(
        self,
        *,
        contract: OpenDataContractStandard,
        payload: ObservationPayload,
    ) -> ValidationResult:  # type: ignore[override]
        self.payloads.append(payload)
        metrics = dict(payload.metrics or {})
        schema = dict(payload.schema or {})
        errors: List[str] = []
        warnings: List[str] = []

        negative = int(metrics.get("violations.non_negative_value", 0) or 0)
        if negative:
            errors.append(f"{negative} negative values detected")

        missing_value = int(metrics.get("violations.value_not_null", 0) or 0)
        if missing_value:
            errors.append(f"{missing_value} rows missing 'value'")

        schema_columns = set(schema)
        missing_columns = sorted(self.expected_columns - schema_columns)
        extra_columns = sorted(schema_columns - self.expected_columns)
        if missing_columns:
            warnings.append(
                "schema missing columns: " + ", ".join(missing_columns)
            )
        if extra_columns:
            warnings.append(
                "schema has unexpected columns: " + ", ".join(extra_columns)
            )

        return ValidationResult(
            ok=not errors,
            errors=errors,
            warnings=warnings,
            metrics=metrics,
            schema=schema,
        )


class ConsoleGovernanceService:
    """Governance stub that records every dataset evaluation."""

    class Assessment:
        def __init__(self, status: ValidationResult) -> None:
            self.status = status
            self.draft = False

    def __init__(self) -> None:
        self.evaluate_calls: List[Mapping[str, object]] = []

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
    ) -> "ConsoleGovernanceService.Assessment":  # type: ignore[override]
        payload = observations() if callable(observations) else observations
        record = {
            "contract_id": contract_id,
            "contract_version": contract_version,
            "dataset_id": dataset_id,
            "dataset_version": dataset_version,
            "validation": validation,
            "payload": payload,
            "operation": operation,
        }
        self.evaluate_calls.append(record)
        print(
            f"[governance] observed {dataset_id}@{dataset_version} status={validation.status}"
        )
        return ConsoleGovernanceService.Assessment(
            ValidationResult(ok=True, status="ok")
        )

    def review_validation_outcome(self, **_kwargs):  # type: ignore[override]
        return None

    def link_dataset_contract(self, **_kwargs) -> None:  # type: ignore[override]
        return None


@dataclass
class ScenarioEnvironment:
    spark: SparkSession
    contract: OpenDataContractStandard
    contract_service: LocalContractServiceClient
    dq_service: ScenarioDQService
    governance_service: ConsoleGovernanceService
    workspace: Path

    @property
    def checkpoint_dir(self) -> Path:
        base = self.workspace / "checkpoints"
        base.mkdir(parents=True, exist_ok=True)
        return base

    def reader_locator(self) -> StaticDatasetLocator:
        return StaticDatasetLocator(
            dataset_id=self.contract.id,
            dataset_version=None,
            format="rate",
        )

    def writer_locator(self, *, dataset_id: Optional[str] = None) -> StaticDatasetLocator:
        return StaticDatasetLocator(
            dataset_id=dataset_id or self.contract.id,
            format="memory",
        )


def build_environment(spark: SparkSession) -> ScenarioEnvironment:
    workspace = Path(tempfile.mkdtemp(prefix="dc43_stream_demo_"))
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
                    SchemaProperty(
                        name="timestamp", physicalType="timestamp", required=True
                    ),
                    SchemaProperty(
                        name="value", physicalType="bigint", required=True
                    ),
                ],
            )
        ],
        servers=[Server(server="local", type="stream", format="rate")],
    )
    store = FSContractStore(str(workspace / "contracts"))
    store.put(contract)
    contract_service = LocalContractServiceClient(store)
    dq_service = ScenarioDQService(contract)
    governance_service = ConsoleGovernanceService()
    return ScenarioEnvironment(
        spark=spark,
        contract=contract,
        contract_service=contract_service,
        dq_service=dq_service,
        governance_service=governance_service,
        workspace=workspace,
    )


def drive_queries(
    queries: Iterable[StreamingQuery],
    *,
    seconds: int,
) -> Optional[StreamingQueryException]:
    deadline = time.time() + seconds
    while time.time() < deadline:
        for query in queries:
            try:
                query.processAllAvailable()
            except StreamingQueryException as exc:  # pragma: no cover - runtime behaviour
                return exc
        time.sleep(0.5)
    return None


def stop_queries(queries: Iterable[StreamingQuery]) -> None:
    for query in queries:
        try:
            if query.isActive:
                query.stop()
        except Exception:  # pragma: no cover - defensive cleanup
            continue


def summarise_batches(dq_service: ScenarioDQService) -> None:
    if not dq_service.payloads:
        print("no batches observed yet")
        return
    print("recorded micro-batches:")
    for index, payload in enumerate(dq_service.payloads, start=1):
        metrics = {k: v for k, v in payload.metrics.items() if not k.startswith("schema")}
        print(f"  batch {index}: metrics={metrics}")


def show_memory_table(spark: SparkSession, table: str) -> None:
    try:
        spark.sql(f"SELECT * FROM {table}").show(truncate=False)
    except Exception:  # pragma: no cover - presentation helper
        print(f"table '{table}' not available")


def scenario_valid(spark: SparkSession, *, seconds: int) -> None:
    print("=== Valid streaming pipeline ===")
    env = build_environment(spark)

    df, read_status = read_stream_with_contract(
        spark=spark,
        contract_id=env.contract.id,
        contract_service=env.contract_service,
        expected_contract_version=f"=={env.contract.version}",
        data_quality_service=env.dq_service,
        governance_service=env.governance_service,
        dataset_locator=env.reader_locator(),
        options={"rowsPerSecond": "4", "numPartitions": "1"},
    )
    if read_status is not None:
        details = read_status.details
        print(
            f"read dataset version: {details.get('dataset_id')}@{details.get('dataset_version')}"
        )

    write_result = write_stream_with_contract(
        df=df,
        contract_id=env.contract.id,
        contract_service=env.contract_service,
        expected_contract_version=f"=={env.contract.version}",
        data_quality_service=env.dq_service,
        governance_service=env.governance_service,
        dataset_locator=env.writer_locator(),
        format="memory",
        options={
            "queryName": "valid_events",
            "checkpointLocation": str(env.checkpoint_dir / "valid"),
        },
    )

    queries = list(write_result.details.get("streaming_queries") or [])
    exception = drive_queries(queries, seconds=seconds)
    if exception:
        print(f"stream halted unexpectedly: {exception}")
    else:
        print("stream processed without errors")
    summarise_batches(env.dq_service)
    details = write_result.details
    print(
        "latest validation:",
        {
            "ok": write_result.ok,
            "dataset_version": details.get("dataset_version"),
            "streaming_batch_id": details.get("streaming_batch_id"),
            "streaming_metrics": details.get("streaming_metrics"),
        },
    )
    show_memory_table(spark, "valid_events")
    stop_queries(queries)
    print()


def scenario_dq_rejects(spark: SparkSession, *, seconds: int) -> None:
    print("=== Streaming with data-quality rejects ===")
    env = build_environment(spark)

    source_df, _ = read_stream_with_contract(
        spark=spark,
        contract_id=env.contract.id,
        contract_service=env.contract_service,
        expected_contract_version=f"=={env.contract.version}",
        data_quality_service=env.dq_service,
        governance_service=env.governance_service,
        dataset_locator=env.reader_locator(),
        options={"rowsPerSecond": "6", "numPartitions": "1"},
    )

    mutated_df = source_df.withColumn(
        "value",
        F.when(F.col("value") % 4 == 0, -F.col("value")).otherwise(F.col("value")),
    )

    write_result = write_stream_with_contract(
        df=mutated_df,
        contract_id=env.contract.id,
        contract_service=env.contract_service,
        expected_contract_version=f"=={env.contract.version}",
        data_quality_service=env.dq_service,
        governance_service=env.governance_service,
        dataset_locator=env.writer_locator(),
        format="memory",
        options={
            "queryName": "dq_events",
            "checkpointLocation": str(env.checkpoint_dir / "dq"),
        },
        enforce=False,
    )

    reject_query = (
        mutated_df.filter(F.col("value") < 0)
        .writeStream.format("memory")
        .queryName("dq_events_reject")
        .outputMode("append")
        .option("checkpointLocation", str(env.checkpoint_dir / "reject"))
        .start()
    )

    queries = list(write_result.details.get("streaming_queries") or [])
    queries.append(reject_query)
    exception = drive_queries(queries, seconds=seconds)
    if exception:
        print(f"stream halted unexpectedly: {exception}")
    else:
        print("stream continued despite quality failures")

    summarise_batches(env.dq_service)
    details = write_result.details
    print(
        "latest validation:",
        {
            "ok": write_result.ok,
            "errors": write_result.errors,
            "dataset_version": details.get("dataset_version"),
            "streaming_metrics": details.get("streaming_metrics"),
        },
    )

    try:
        reject_count = spark.sql("SELECT COUNT(*) FROM dq_events_reject").collect()[0][0]
    except Exception:
        reject_count = 0
    print(f"rows captured in reject sink: {reject_count}")
    show_memory_table(spark, "dq_events_reject")
    stop_queries(queries)
    print()


def scenario_schema_break(spark: SparkSession, *, seconds: int) -> None:
    print("=== Schema break detection ===")
    env = build_environment(spark)

    source_df, _ = read_stream_with_contract(
        spark=spark,
        contract_id=env.contract.id,
        contract_service=env.contract_service,
        expected_contract_version=f"=={env.contract.version}",
        data_quality_service=env.dq_service,
        governance_service=env.governance_service,
        dataset_locator=env.reader_locator(),
        options={"rowsPerSecond": "4", "numPartitions": "1"},
    )

    broken_df = source_df.drop("value")

    write_result = write_stream_with_contract(
        df=broken_df,
        contract_id=env.contract.id,
        contract_service=env.contract_service,
        expected_contract_version=f"=={env.contract.version}",
        data_quality_service=env.dq_service,
        governance_service=env.governance_service,
        dataset_locator=env.writer_locator(),
        format="memory",
        options={
            "queryName": "schema_break",
            "checkpointLocation": str(env.checkpoint_dir / "schema"),
        },
    )

    queries = list(write_result.details.get("streaming_queries") or [])
    exception = drive_queries(queries, seconds=seconds)
    if exception:
        print(f"stream halted after schema change: {exception}")
    else:
        print("stream processed without raising an error")

    summarise_batches(env.dq_service)
    details = write_result.details
    print(
        "latest validation:",
        {
            "ok": write_result.ok,
            "errors": write_result.errors,
            "dataset_version": details.get("dataset_version"),
            "streaming_metrics": details.get("streaming_metrics"),
        },
    )
    stop_queries(queries)
    print()


def build_spark(app_name: str) -> SparkSession:
    spark = (
        SparkSession.builder.master("local[2]")
        .appName(app_name)
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "1")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


def main(argv: Sequence[str]) -> int:
    parser = argparse.ArgumentParser(
        description="Demonstrate dc43 streaming contract integrations",
    )
    parser.add_argument(
        "scenario",
        choices=["valid", "dq-rejects", "schema-break", "all"],
        help="scenario to run",
    )
    parser.add_argument(
        "--seconds",
        type=int,
        default=5,
        help="duration spent driving micro-batches for each scenario",
    )
    args = parser.parse_args(argv)

    spark = build_spark("dc43-streaming-scenarios")
    try:
        if args.scenario in {"valid", "all"}:
            scenario_valid(spark, seconds=args.seconds)
        if args.scenario in {"dq-rejects", "all"}:
            scenario_dq_rejects(spark, seconds=args.seconds)
        if args.scenario in {"schema-break", "all"}:
            scenario_schema_break(spark, seconds=args.seconds)
    finally:
        spark.stop()
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
