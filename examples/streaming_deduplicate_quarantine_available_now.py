#!/usr/bin/env python3
"""Example: Structured Streaming with Deduplication, Quarantine, and availableNow trigger.

This example demonstrates how to ingest streaming data using dc43's
`write_stream_with_governance` (or `write_with_governance`) paired with the built-in
`DeduplicateQuarantineWriteViolationStrategy`.

Under the hood:
1. `dc43` detects that `DeduplicateQuarantineWriteViolationStrategy` requires micro-batch
   isolation (`requires_micro_batch=True`) and encapsulates `df.writeStream.foreachBatch`.
2. Inside each micro-batch, data is ranked using window functions:
   `ROW_NUMBER() OVER (PARTITION BY <key_properties> ORDER BY <sort_column> DESC)`.
3. Valid (rank == 1) records are appended to the primary target table.
4. Duplicate (rank > 1) records are routed to the quarantine table (`_reject`), optionally
   aggregated into a list column (`quarantined_duplicates`) with `duplicate_count`.
5. The `availableNow=True` trigger consumes all available backlogged data across micro-batches
   and terminates all streaming queries cleanly once processing completes.
"""

from __future__ import annotations

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from dc43_integrations.spark.io import (
    GovernanceSparkWriteRequest,
    write_stream_with_governance,
)
from dc43_integrations.spark.violation_strategy import (
    DeduplicateQuarantineWriteViolationStrategy,
)
from dc43_service_clients.governance.models import (
    ContractReference,
    GovernanceWriteContext,
    PipelineContext,
)


def run_streaming_dedup_pipeline(
    spark: SparkSession,
    governance_service,  # Initialized GovernanceServiceClient
    landing_path: str = "/mnt/landing/orders_raw",
    checkpoint_path: str = "/mnt/checkpoints/orders_dedup",
    target_table: str = "catalog.sales.orders",
) -> None:
    """Run an incremental, governed streaming ingestion with deduplication."""

    # 1. Read input stream with watermark
    streaming_df = (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "json")
        .load(landing_path)
        .withWatermark("event_ts", "10 minutes")
    )

    # 2. Configure the built-in deduplicate & quarantine violation strategy
    violation_strategy = DeduplicateQuarantineWriteViolationStrategy(
        key_properties=["tenant_id", "order_id"],  # Composite primary key
        sort_column="updated_at",                  # Ordering column for arbitration
        keep="first",                              # Keep newest record (updated_at desc)
        reject_suffix="reject",                    # Quarantine table: catalog.sales.orders_reject
        aggregate_quarantine=True,                 # Group duplicates per key into struct array
        # Optional: Per-dataset DataFrameWriter modifiers inside each micro-batch
        valid_writer_modifier=lambda writer: writer.option("optimizeWrite", "true"),
        reject_writer_modifier=lambda writer: writer.option("compression", "snappy"),
    )

    # 3. Build the governed write request (table and format are resolved directly from the contract)
    request = GovernanceSparkWriteRequest(
        context=GovernanceWriteContext(
            contract=ContractReference(
                contract_id="sales.orders",
                version_selector="1.0.0",
            ),
            pipeline_context=PipelineContext(pipeline="orders_streaming_ingest"),
        ),
        options={
            "checkpointLocation": checkpoint_path,
        },
        violation_strategy=violation_strategy,
        # Stream-level configuration: availableNow processes all pending data and shuts down
        writer_modifier=lambda stream_writer: stream_writer.trigger(availableNow=True),
    )

    # 4. Execute the stream under dc43 governance
    print(f"Starting governed streaming write to {target_table}...")
    execution_result = write_stream_with_governance(
        df=streaming_df,
        request=request,
        governance_service=governance_service,
        enforce=False,
    )

    # 5. Await termination (for trigger(availableNow=True), this exits once all data is processed)
    for query in execution_result.streaming_queries:
        print(f"Waiting for streaming query {query.name or query.id} to finish...")
        query.awaitTermination()

    print("Incremental streaming batch completed successfully!")

    # 6. Verify the results
    primary_count = spark.table(target_table).count()
    reject_table = f"{target_table}_reject"
    reject_count = spark.table(reject_table).count()
    print(f"Primary table ({target_table}) row count: {primary_count}")
    print(f"Quarantine table ({reject_table}) row count: {reject_count}")


if __name__ == "__main__":
    spark = SparkSession.builder.appName("dc43-streaming-dedup").getOrCreate()
    # Replace with your actual GovernanceServiceClient instance
    # from dc43_service_backends.bootstrap import build_governance_service
    # governance_service = build_governance_service(...)
    print("Example ready to run on Databricks or local Spark cluster.")
