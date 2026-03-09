# Writing Data with Governance

Writing datasets reliably is the core of `dc43-integrations`. The `write_with_governance` API evaluates both schema and data quality expectations, applies policies (like breaking the build or generating draft contracts on drift), and updates your Governance Catalogue.

## The Core Function: `write_with_governance`

Replace standard `df.write` with `write_with_governance` to inject the validation layer.

```python
from dc43_integrations.spark.io import write_with_governance, GovernanceSparkWriteRequest
from dc43_integrations.spark.strategy import GovernanceWriteContext, GovernancePolicy
from dc43_service_clients.governance.models import ContractReference, PipelineContext

request = GovernanceSparkWriteRequest(
    context=GovernanceWriteContext(
        # We look up the existing contract to govern this write
        contract=ContractReference(contract_id="test.orders", version_selector="1.0.0"),
        pipeline_context=PipelineContext(pipeline="daily_ingest"),
        policy=GovernancePolicy(draft_on_violation=True) 
    ),
    # By default, the Governance client resolves the physical output port based on the contract!
    # Explicit sink paths are only required if you are overriding standard convention:
    # path="s3://lake/orders_output", 
    # format="parquet",
)

# df is your Spark DataFrame
execution_result = write_with_governance(
    df=df,
    request=request,
    governance_service=my_governance_client,
    enforce=True, # Block the write if validation fails
    auto_cast=True, # Re-order and cast columns to match the contract
)

# Check the outcome
if not execution_result.validation.ok:
    print(f"Warnings: {execution_result.validation.warnings}")
```

## How It Works

1. **Alignment**: The DataFrame columns are re-ordered and cast to match the exact contract specification.
2. **Quality Evaluation**: Spark computes metrics based on the data expectations defined in the Data Contract.
3. **Governance Assessment**: The integration hands the metrics over to the `governance_service`. The service decides if the payload meets the contract standard.
4. **Sink Writing**: If everything passes (or if `enforce=False`), the aligned data is written to the destination sink.

## Streaming Writers

`write_with_governance` automatically supports `df.writeStream` if the input `df` is a streaming DataFrame.

When a streaming write is executed, the helper launches a dedicated observation writer that evaluates contract expectations for *each micro-batch* (via `foreachBatch`), forwarding the live metrics to the governance service.

### Intervention Strategies

For streaming workloads, you can provide a `StreamingInterventionStrategy` directly to the `write_with_governance` request via `streaming_intervention_strategy`. This allows you to block the pipeline, surface warnings, or trigger custom routing on repeated micro-batch failures.
