# Spark streaming integration

The Spark helpers in `dc43_integrations` can build Structured Streaming
pipelines that continue to enforce Open Data Contracts and coordinate with the
data-quality and governance services.  Batch APIs remain unchanged while
dedicated helpers such as `read_stream_with_contract`,
`read_stream_from_contract`, and `write_stream_with_contract` route through
`SparkSession.readStream` / `DataFrame.writeStream` without threading a flag
through the batch code paths.

Streaming IO still performs contract validation and posts schema observations to
governance.  When a streaming write is executed the helper also launches a
dedicated observation writer that materialises the contract expectations for
each micro-batch via `foreachBatch`, forwarding the resulting metrics back to
the attached data-quality client and updating the returned validation payload.
Reads remain schema-only until a sink materialises the stream, but write
validations now produce live metrics without blocking the ingestion job.  The
validation result contains every `streaming_query` started by the helpers so
callers can manage their lifecycle.  Read and write validations also expose the
`dataset_id` and `dataset_version` they submit to governance so micro-batch
monitors can query the data-quality service for the latest verdict or trigger
asynchronous metric computation when a streaming snapshot needs to be
inspected.

Observation writers are single-use helpers and can be paired with a
`StreamingInterventionStrategy` to take action after repeated issues: the
strategy receives the validation result for every batch and can decide to block
the pipeline, surface warnings, or trigger alternative routing (for example by
switching over to a rejection sink).  The default implementation never blocks,
preserving the previous behaviour when no strategy is supplied.

```python
from pyspark.sql import SparkSession
from dc43_service_backends.contracts.backend.stores import FSContractStore
from dc43_service_clients.contracts import LocalContractServiceClient
from dc43_service_clients.data_quality import ObservationPayload, ValidationResult
from dc43_integrations.spark.io import (
    StaticDatasetLocator,
    StreamingInterventionContext,
    StreamingInterventionStrategy,
    read_stream_with_contract,
    write_stream_with_contract,
)
from open_data_contract_standard.model import (
    Description,
    OpenDataContractStandard,
    SchemaObject,
    SchemaProperty,
    Server,
)


class NoOpDQService:
    def describe_expectations(self, *, contract):
        return []

    def evaluate(self, *, contract, payload: ObservationPayload) -> ValidationResult:
        return ValidationResult(ok=True, metrics=payload.metrics, schema=payload.schema)


class BlockAfterFailures(StreamingInterventionStrategy):
    def __init__(self, *, max_failures: int = 3) -> None:
        self._failures = 0
        self._max_failures = max_failures

    def decide(self, context: StreamingInterventionContext):
        if not context.validation.ok:
            self._failures += 1
            if self._failures >= self._max_failures:
                return (
                    f"halt after {self._failures} failed batches for "
                    f"{context.dataset_id}@{context.dataset_version}"
                )
        return None


spark = (
    SparkSession.builder.master("local[2]")
    .appName("dc43-stream-demo")
    .config("spark.ui.enabled", "false")
    .getOrCreate()
)

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

store = FSContractStore("/tmp/contracts")
store.put(contract)
contract_service = LocalContractServiceClient(store)
dq_service = NoOpDQService()

stream_df, read_status = read_stream_with_contract(
    spark=spark,
    contract_id=contract.id,
    contract_service=contract_service,
    expected_contract_version=f"=={contract.version}",
    data_quality_service=dq_service,
    dataset_locator=StaticDatasetLocator(format="rate"),
    options={"rowsPerSecond": "5"},
)

write_result = write_stream_with_contract(
    df=stream_df,
    contract_id=contract.id,
    contract_service=contract_service,
    expected_contract_version=f"=={contract.version}",
    data_quality_service=dq_service,
    dataset_locator=StaticDatasetLocator(format="memory"),
    format="memory",
    options={"queryName": "rate_events"},
    streaming_intervention_strategy=BlockAfterFailures(max_failures=2),
)

for query in write_result.details["streaming_queries"]:
    query.processAllAvailable()
    query.stop()
```

Data-product bindings participate in the same streaming workflow: registering a
streaming port via `read_stream_from_data_product`, `read_stream_with_contract`,
or the streaming `write_*` counterparts will validate the contract, update the
governance catalogue, and return any active `StreamingQuery` handles through the
validation payload.
