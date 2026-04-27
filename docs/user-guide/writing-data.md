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
    
    # Advanced: Use a modifier function to apply Spark-specific configurations (like partitionBy or trigger)
    # writer_modifier=lambda w: w.partitionBy("date").trigger(availableNow=True)
    
    # Optional: Apply an ordered sequence of lifecycle interceptors
    # interceptors=["utils.pii:PrivacyInterceptor", "utils.enrichment:UnityCatalogInterceptor"]
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

1. **Interceptors**: Any defined interceptors (passed explicitly via `interceptors` or configured globally via `DC43_GOVERNANCE_INTERCEPTORS`) are applied to the DataFrame lifecycle.
   * **Sequence Order**: `pre_write` hooks (like PII masking) are executed before the schema alignment. After standard constraints are checked, data is written. Upon successful persist, `post_write` hooks (like Unity Catalog tagging) apply.
2. **Alignment**: The DataFrame columns are re-ordered and cast to match the exact contract specification.
3. **Quality Evaluation**: Spark computes metrics based on the data expectations defined in the Data Contract.
4. **Governance Assessment**: The integration hands the metrics over to the `governance_service`. The service decides if the payload meets the contract standard.
5. **Sink Writing**: If everything passes (or if `enforce=False`), the aligned data is written to the destination sink.

## Streaming Writers

`write_with_governance` automatically supports `df.writeStream` if the input `df` is a streaming DataFrame.

When a streaming write is executed, the helper launches a dedicated observation writer that evaluates contract expectations for *each micro-batch* (via `foreachBatch`), forwarding the live metrics to the governance service.

### Intervention Strategies

For streaming workloads, you can provide a `StreamingInterventionStrategy` directly to the `write_with_governance` request via `streaming_intervention_strategy`. This allows you to block the pipeline, surface warnings, or trigger custom routing on repeated micro-batch failures.

## Merging Data with Governance (Delta Lake)

If your destination sink is a Delta Lake table and you need to perform Upserts (Merges), use the `merge_with_governance` API. It provides the exact same governance flow as `write_with_governance` but intercepts the Spark operation to perform a Delta merge instead.

```python
from dc43_integrations.spark.io import merge_with_governance

def merge_modifier(builder):
    return builder.whenMatchedUpdateAll().whenNotMatchedInsertAll()

execution_result = merge_with_governance(
    source_df=source_df,
    condition="target.id = source.id",
    request=request, # GovernanceSparkWriteRequest
    governance_service=my_governance_client,
    merge_builder_modifier=merge_modifier,
    enforce=True,
)
```

The data quality expectations are verified on the `source_df` prior to executing the merge operation on the target. The target table is automatically resolved from the contract.

## Declaring Governed Views

In addition to writing physical data with DataFrames, the `dc43` Spark integration allows you to declare persistent Databricks views using the `declare_with_governance` API. This evaluates inputs dynamically and outputs SQL for robust, governed deployments.

```python
from dc43_integrations.spark.io import declare_with_governance, GovernanceSparkDeclareRequest
from dc43_integrations.spark.io.common import GovernanceSparkReadRequest

# The {input_dataset} acts as a placeholder that the framework will securely 
# resolve into a Databricks catalog or delta path.
sql_template = """
    SELECT id, value * 100 as percentage 
    FROM {input_dataset} 
    WHERE status = 'active'
"""

execution_result = declare_with_governance(
    spark=spark,
    sql_template=sql_template,
    inputs={
        # Inputs undergo complete governance evaluation. Time travel is resolved automatically 
        # generating queries executing e.g., \"my_table VERSION AS OF x\".
        "input_dataset": GovernanceSparkReadRequest(dataset_id="source-dataset-id") 
    },
    request=GovernanceSparkDeclareRequest(
        dataset_id="destination-view-id"
    ),
    governance_service=my_governance_client,
    enforce=True, # Will block View creation if the input dataset breaches Data Quality rules!
)
```

**Key Behaviors for Views:**
* **Pre-flight Evaluation**: Unlike traditional tables where validation occurs *during* the write, view input dependencies are queried and validated *before* the `CREATE VIEW` statement executes. If an input breaches its quality or status policy, the view declaration aborts.
* **Smart Translation**: `declare_with_governance` automatically translates `DatasetLocator` definitions (such as `ContractVersionLocator` alias resolutions) into pure Databricks SQL syntaxes, accurately injecting `VERSION AS OF` options directly into the SQL string.
* **Metastore Enforcement**: Databricks prohibits permanent views anchored to pure filesystem paths. Ensure that the associated output contract provides a standard `catalog.schema.table` mapping.
