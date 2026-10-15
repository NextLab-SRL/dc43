#!/usr/bin/env python3
"""Example: Structured Streaming Upserts (Merges) using merge_stream_with_governance.

This example demonstrates how to perform governed Delta Lake merges from a Spark
Structured Streaming source using `merge_stream_with_governance` (or `merge_with_governance`).

Under the hood:
1. `dc43` detects that `source_df.isStreaming == True` and encapsulates `writeStream.foreachBatch`.
2. Target Delta Lake tables are automatically pre-created using `ContractDDLBuilder` if they
   do not exist, strictly matching the contract schema, constraints (`NOT NULL`, `PRIMARY KEY`),
   clustering (`CLUSTER BY`), and table properties.
3. For each non-empty micro-batch, the Delta merge builder applies configured matched update
   and unmatched insert clauses.
4. Triggers such as `availableNow=True` or continuous intervals are applied at the stream level
   via `request.writer_modifier`.
"""

from __future__ import annotations

from pyspark.sql import SparkSession

from dc43_integrations.spark.io import (
    GovernanceSparkWriteRequest,
    merge_stream_with_governance,
)
from dc43_service_clients.governance.models import (
    ContractReference,
    GovernanceWriteContext,
    PipelineContext,
)


def run_streaming_merge_pipeline(
    spark: SparkSession,
    governance_service,  # Initialized GovernanceServiceClient
    landing_path: str = "/mnt/landing/customers_cdc",
    checkpoint_path: str = "/mnt/checkpoints/customers_merge",
    target_table: str = "catalog.crm.customers",
) -> None:
    """Run an incremental, governed streaming Delta Lake merge."""

    # 1. Read CDC / incremental stream
    streaming_df = (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "parquet")
        .load(landing_path)
    )

    # 2. Build the governed write request (table and format are resolved directly from the contract)
    request = GovernanceSparkWriteRequest(
        context=GovernanceWriteContext(
            contract=ContractReference(
                contract_id="crm.customers",
                version_selector="1.0.0",
            ),
            pipeline_context=PipelineContext(pipeline="customers_cdc_merge"),
        ),
        options={
            "checkpointLocation": checkpoint_path,
        },
        # Trigger availableNow: processes all available pending data and terminates
        writer_modifier=lambda stream_writer: stream_writer.trigger(availableNow=True),
        # Optional: Delta table properties to apply when table is created for the first time
        table_properties={
            "delta.autoOptimize.optimizeWrite": "true",
            "delta.enableChangeDataFeed": "true",
        },
    )

    # 3. Define merge match/insert clauses
    def configure_merge_builder(builder):
        return (
            builder.whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
        )

    # 4. Execute the streaming merge under dc43 governance
    print(f"Starting governed streaming merge into {target_table}...")
    execution_result = merge_stream_with_governance(
        source_df=streaming_df,
        condition="target.customer_id = source.customer_id",
        request=request,
        governance_service=governance_service,
        merge_builder_modifier=configure_merge_builder,
        enforce=False,
    )

    # 5. Await termination
    for query in execution_result.streaming_queries:
        print(f"Waiting for streaming query {query.name or query.id} to finish...")
        query.awaitTermination()

    print("Streaming merge completed successfully!")


if __name__ == "__main__":
    spark = SparkSession.builder.appName("dc43-streaming-merge").getOrCreate()
    print("Example ready to run on Databricks or local Spark cluster.")
