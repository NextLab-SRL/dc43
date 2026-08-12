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
from dc43_service_clients.governance.models import GovernanceReadContext, GovernanceWriteContext

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
        # Inputs undergo complete governance evaluation. We recommend a "Data Contract First" 
        # (or "Data Product/Port First") approach rather than low-level dataset IDs.
        "input_dataset": GovernanceSparkReadRequest(
            context=GovernanceReadContext.from_contract(id="sales.orders", version="1.0.0")
        ) 
    },
    request=GovernanceSparkDeclareRequest(
        # We target the output contract to govern this view's schema and properties
        context=GovernanceWriteContext.from_contract(id="sales.orders_taxed_view")
    ),
    governance_service=my_governance_client,
    enforce=True, # Will block View creation if the input dataset breaches Data Quality rules!
)
```

**Key Behaviors for Views:**
* **Pre-flight Evaluation**: Unlike traditional tables where validation occurs *during* the write, view input dependencies are queried and validated *before* the `CREATE VIEW` statement executes. If an input breaches its quality or status policy, the view declaration aborts.
* **Smart Translation**: `declare_with_governance` automatically translates `DatasetLocator` definitions (such as `ContractVersionLocator` alias resolutions) into pure Databricks SQL syntaxes, accurately injecting `VERSION AS OF` options directly into the SQL string.
* **Metastore Enforcement**: Databricks prohibits permanent views anchored to pure filesystem paths. Ensure that the associated output contract provides a standard `catalog.schema.table` mapping.

## Data Quality Metrics (ODCS v3.1.0)

`dc43` natively extracts and evaluates the full set of ODCS v3.1.0 standard metrics:

| Metric | Level | Description | Arguments / Example |
| :--- | :--- | :--- | :--- |
| `nullValues` | Property | Counts null values in a field | `metric: nullValues, mustBe: 0` |
| `missingValues` | Property | Counts values considered as missing | `metric: missingValues, arguments: { missingValues: [null, '', 'N/A'] }` |
| `invalidValues` | Property | Counts values failing valid criteria (enum or regex) | `metric: invalidValues, arguments: { validValues: ['EUR', 'USD'] }` |
| `duplicateValues` | Property | Counts duplicate values in a single column | `metric: duplicateValues, mustBe: 0` |
| `duplicateValues` | Schema | Counts duplicate rows across compound key properties | `metric: duplicateValues, arguments: { properties: ['tenant_id', 'order_id'] }` |
| `rowCount` | Schema | Validates dataset total row count thresholds | `metric: rowCount, mustBeGreaterThan: 0` |

### ODCS Contract Example

```yaml
kind: DataContract
apiVersion: 3.1.0
id: sales.orders
schema:
  - name: orders
    quality:
      - id: orders_unique_tenant_order
        description: The combination of tenant_id and order_id must be unique
        metric: duplicateValues
        mustBe: 0
        arguments:
          properties:
            - tenant_id
            - order_id
      - id: orders_min_rowCount
        metric: rowCount
        mustBeGreaterThan: 0
    properties:
      - name: tenant_id
        type: string
        quality:
          - metric: nullValues
            mustBe: 0
      - name: order_id
        type: string
        quality:
          - metric: duplicateValues
            mustBe: 0
      - name: status
        type: string
        quality:
          - metric: missingValues
            mustBe: 0
            arguments:
              missingValues: [null, '', 'N/A']
```

## Violation Strategies & Duplicate Quarantine

When write validation detects quality or schema violations, `dc43` uses violation strategies to decide how data flows to target sinks.

### Split Quarantine (`SplitWriteViolationStrategy`)

The `SplitWriteViolationStrategy` splits invalid rows from valid rows during write:

```python
from dc43_integrations.spark.violation_strategy import SplitWriteViolationStrategy

request = GovernanceSparkWriteRequest(
    context=GovernanceWriteContext(
        contract=ContractReference(contract_id="sales.orders", version_selector="1.0.0"),
        policy=GovernancePolicy(draft_on_violation=True),
    ),
    violation_strategy=SplitWriteViolationStrategy(
        valid_suffix="valid",
        reject_suffix="reject",
        include_valid=True,
        include_reject=True,
    ),
)
```

### Custom Duplicate Quarantine Strategy (`DeduplicateQuarantineWriteViolationStrategy`)

In the `dc43` architecture, metrics collect contract violation counts (`ValidationResult`), while `WriteViolationStrategy` implementations decide how to process, split, or quarantine violating records.

To handle `duplicateValues` rules, you can create a custom `WriteViolationStrategy` that deduplicates the primary dataset based on a sorter instruction (e.g. keeping `first` or `last` record according to an ordering column like `updated_at`), while routing all rejected duplicates to a quarantine sink.

Optionally, the strategy can **aggregate** all quarantined duplicates for a given key into a single row containing a list column (`quarantined_duplicates`) and the count of duplicates (`duplicate_count`).

```python
from dataclasses import dataclass
from typing import Literal, Sequence, Optional
from pyspark.sql import Window
from pyspark.sql import functions as F

from dc43_integrations.spark.violation_strategy import (
    WriteViolationStrategy,
    WriteStrategyContext,
    WritePlan,
    WriteRequest,
)

@dataclass
class DeduplicateQuarantineWriteViolationStrategy:
    """Strategy that deduplicates records on compound keys and routes rejected duplicates to quarantine."""

    key_properties: Sequence[str]
    sort_column: str
    keep: Literal["first", "last"] = "first"
    aggregate_quarantine: bool = False
    valid_suffix: str = "valid"
    reject_suffix: str = "reject"
    dataset_suffix_separator: str = "::"

    def plan(self, context: WriteStrategyContext) -> WritePlan:
        df = context.aligned_df
        
        # 1. Define window ordering (first -> descending, last -> ascending)
        sort_order = F.col(self.sort_column).desc() if self.keep == "first" else F.col(self.sort_column).asc()
        window_spec = Window.partitionBy(*self.key_properties).orderBy(sort_order)
        
        # 2. Assign rank per composite key
        df_ranked = df.withColumn("_row_num", F.row_number().over(window_spec))
        
        # Primary valid records (rank == 1)
        valid_df = df_ranked.filter(F.col("_row_num") == 1).drop("_row_num")
        
        # Quarantined duplicate records (rank > 1)
        reject_df = df_ranked.filter(F.col("_row_num") > 1).drop("_row_num")

        # 3. Optional: Combine all quarantined duplicates into a single aggregated row per key
        if self.aggregate_quarantine:
            other_cols = [c for c in df.columns if c not in self.key_properties]
            reject_df = reject_df.groupBy(*self.key_properties).agg(
                F.collect_list(F.struct(*other_cols)).alias("quarantined_duplicates"),
                F.count("*").alias("duplicate_count")
            )

        def _extend_dataset_id(base: Optional[str], suffix: str) -> Optional[str]:
            return f"{base}{self.dataset_suffix_separator}{suffix}" if base else None

        # Build Primary WriteRequest (clean dataset written to primary sink)
        primary_request = WriteRequest(
            df=valid_df,
            path=context.path,
            table=context.table,
            format=context.format,
            options=dict(context.options),
            mode=context.mode,
            contract=context.contract,
            dataset_id=context.dataset_id,
            dataset_version=context.dataset_version,
            validation_factory=lambda df=valid_df: context.revalidate(df),
        )

        # Build Additional WriteRequest (rejected duplicates written to quarantine sink)
        reject_request = WriteRequest(
            df=reject_df,
            path=f"{context.path}_{self.reject_suffix}" if context.path else None,
            table=f"{context.table}_{self.reject_suffix}" if context.table else None,
            format=context.format,
            options=dict(context.options),
            mode=context.mode,
            contract=context.contract,
            dataset_id=_extend_dataset_id(context.dataset_id, self.reject_suffix),
            dataset_version=context.dataset_version,
        )

        return WritePlan(primary=primary_request, additional=[reject_request])
```

### Using the Custom Strategy

```python
# Pass the custom strategy to write_with_governance
request = GovernanceSparkWriteRequest(
    context=GovernanceWriteContext(
        contract=ContractReference(contract_id="sales.orders", version_selector="1.0.0"),
    ),
    violation_strategy=DeduplicateQuarantineWriteViolationStrategy(
        key_properties=["tenant_id", "order_id"],
        sort_column="updated_at",
        keep="first",                 # Keeps the latest record (newest updated_at)
        aggregate_quarantine=True,    # Groups quarantined duplicates into a list column + duplicate_count
    ),
)

execution_result = write_with_governance(
    df=df,
    request=request,
    governance_service=my_governance_client,
)
```


