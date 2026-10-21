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
    
    # Advanced: Customize or extend generated DDL when creating a table for the first time
    # ddl_modifier=lambda ddl: ddl + "\nTBLPROPERTIES ('delta.autoOptimize.optimizeWrite' = 'true')",
    # table_properties={"delta.enableChangeDataFeed": "true"},
    # constraint_policy=DDLConstraintPolicy.no_nullability(), # Fine-grained DDL constraint tuning
    # compatibility_mode="raise", # "raise" or "warn" for strategy/constraint conflicts
    
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
    print(f"Errors: {execution_result.validation.errors}")
```

## How It Works

1. **Interceptors (Pre-Write)**: Any defined interceptors (passed explicitly via `interceptors` or configured globally via `DC43_GOVERNANCE_INTERCEPTORS`) execute `pre_write` hooks (e.g., PII masking) before schema validation.
2. **Alignment & Quality Evaluation**: The DataFrame columns are re-ordered and cast to match the contract specification. Spark computes metrics based on the data expectations defined in the Data Contract.
3. **Governance Assessment**: The integration hands the metrics over to the `governance_service`.
4. **Hard Gated DDL Pre-Creation**: When writing or merging into a table/storage path that does not exist yet, `dc43` uses `ContractDDLBuilder` to generate and execute the strict `CREATE TABLE IF NOT EXISTS` DDL derived from the contract:
   * **Data Types & Nullability**: Columns and their Spark SQL types with `NOT NULL` constraints (`required: true`).
   * **Primary Keys**: `CONSTRAINT pk_... PRIMARY KEY (...)` for `primaryKey: true` fields (on Delta Lake / Unity Catalog).
   * **Partitioning & Clustering**: `PARTITIONED BY (...)` (`partitioned: true`) or Databricks `CLUSTER BY (...)` (`clustering`).
   * **Table Properties & Prefix Conventions**: `TBLPROPERTIES (...)` populated from contract `customProperties.tableProperties` and `request.table_properties`.
   * **Custom DDL Hook**: `request.ddl_modifier` allows programmatic customization of the DDL string before execution.
5. **Sink Writing**: If validation passes (or if `enforce=False` is set for non-blocking DQ observations), the aligned data is written to the destination sink.
6. **Interceptors (Post-Write) & Error Tracking**: `post_write` hooks (like Unity Catalog tagging) execute. Any failure in post-write hooks is automatically recorded in `execution_result.validation.errors` (and raises an exception if `enforce=True`).

## Contract-to-DDL Conventions & Prefixes

When defining Data Contracts (ODCS), `dc43` adheres to standard property conventions to generate dialect-aware table definitions:

### 1. Table Properties (`customProperties.tableProperties`) & Prefix Scoping

In your ODCS contract YAML, define table properties under `customProperties.tableProperties`:

```yaml
customProperties:
  clustering:
    - customer_id
    - order_date
  tableProperties:
    # Delta Lake specific properties (prefixed with delta.)
    delta.enableChangeDataFeed: "true"
    delta.autoOptimize.optimizeWrite: "true"
    delta.autoOptimize.autoCompact: "true"
    delta.deletedFileRetentionDuration: "interval 30 days"
    
    # Generic metadata / Governance properties (preserved across all formats)
    domain: "finance"
    owner: "data_engineering"
    data_classification: "confidential"
```

#### Prefix Convention Rules:
* **`delta.<property>`**: Scoped specifically to **Delta Lake** tables. When the write destination is `format="delta"`, these are injected directly into `TBLPROPERTIES (...)`. When the destination is a non-Delta format (`parquet`, `orc`), `dc43` **automatically filters out** all `delta.*` properties so the target metastore/catalog does not reject the DDL.
* **`iceberg.<property>` / `write.<property>`**: Scoped to **Apache Iceberg** tables (e.g. `write.format.default='parquet'`).
* **Unprefixed / Generic properties** (e.g. `owner`, `domain`, `classification`): Injected into `TBLPROPERTIES` across all supported table formats.

### 2. Column-Level Constraints & Partitioning

| ODCS Contract Attribute | Generated DDL Clause (Delta) | Generated DDL Clause (Parquet/Standard) |
| :--- | :--- | :--- |
| `required: true` | `col_name type NOT NULL` | `col_name type NOT NULL` |
| `primaryKey: true` | `CONSTRAINT pk_<tbl> PRIMARY KEY (col1, ...)` | *(Omitted - not supported in pure Hive/Parquet DDL)* |
| `partitioned: true` | `PARTITIONED BY (col1, ...)` | `PARTITIONED BY (col1, ...)` |
| `customProperties.clustering: [...]` | `CLUSTER BY (col1, ...)` *(Liquid Clustering)* | *(Falls back to `PARTITIONED BY` if defined)* |
| `description: "..."` | `col_name type COMMENT '...'` | `col_name type COMMENT '...'` |
| `path: "/mnt/lake/table"` (no table name) | `CREATE TABLE delta.\`/mnt/lake/table\`` | `CREATE TABLE \`table\` ... LOCATION '/mnt/lake/table'` |

### 3. Declarative Constraint Tuning & Strategy Compatibility

To selectively tune physical DDL constraints (instead of all-or-nothing generation), pass a `DDLConstraintPolicy` to `GovernanceSparkWriteRequest` or `ContractDDLBuilder`:

```python
from dc43_integrations.spark.ddl_constraints import DDLConstraintPolicy, DDLConstraintType

# Disable NOT NULL constraints globally or on specific columns
policy = (
    DDLConstraintPolicy.strict()
    .with_column_override("order_date", DDLConstraintType.NOT_NULL, enabled=False)
)

request = GovernanceSparkWriteRequest(
    contract=contract,
    constraint_policy=policy,
    compatibility_mode="raise", # Detects conflicts between DDL invariants and violation strategies
)
```

For complete details on constraint registries, column overrides, reject tables, and incompatibility detection, see the dedicated [DDL Constraints and Strategy Compatibility Guide](file:///docs/user-guide/ddl-constraints-and-compatibility.md).

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

### Built-in Duplicate Quarantine Strategy (`DeduplicateQuarantineWriteViolationStrategy`)

In the `dc43` architecture, metrics collect contract violation counts (`ValidationResult`), while `WriteViolationStrategy` implementations decide how to process, split, or quarantine violating records.

To handle `duplicateValues` rules, `dc43-integrations` provides a built-in `DeduplicateQuarantineWriteViolationStrategy` exported from `dc43_integrations.spark.violation_strategy`. It deduplicates the primary dataset based on a sort instruction (e.g. keeping `first` or `last` record according to an ordering column like `updated_at` or `order_by=[("updated_at", "DESC")]`), while routing all rejected duplicates to a quarantine sink. If `key_properties` is omitted, the deduplication keys are inferred automatically from the contract (`primaryKey: true` properties or `duplicateValues` quality expectations).

Optionally, the strategy can **aggregate** all quarantined duplicates for a given key into a single row containing a list column (`quarantined_duplicates`) and the count of duplicates (`duplicate_count`).

```python
from dc43_integrations.spark.io import write_with_governance, write_stream_with_governance, GovernanceSparkWriteRequest
from dc43_integrations.spark.violation_strategy import DeduplicateQuarantineWriteViolationStrategy
from dc43_service_clients.governance.models import GovernanceWriteContext, ContractReference

# Configure the deduplication and quarantine strategy
violation_strategy = DeduplicateQuarantineWriteViolationStrategy(
    key_properties=["tenant_id", "order_id"],
    sort_column="updated_at",
    keep="first",                 # Keeps the latest record (newest updated_at)
    aggregate_quarantine=True,    # Groups quarantined duplicates into a list column + duplicate_count
    reject_suffix="reject",
    # Optional: Customize the DataFrameWriter for each split dataset (e.g. partitioning or format options)
    valid_writer_modifier=lambda writer: writer.partitionBy("order_date"),
    reject_writer_modifier=lambda writer: writer.option("compression", "snappy"),
)

request = GovernanceSparkWriteRequest(
    context=GovernanceWriteContext(
        contract=ContractReference(contract_id="sales.orders", version_selector="1.0.0"),
    ),
    violation_strategy=violation_strategy,
)

execution_result = write_with_governance(
    df=df,
    request=request,
    governance_service=my_governance_client,
)
```

### Streaming Micro-Batch Deduplication & Quarantine (`writeStream.foreachBatch`)

When running on Spark Structured Streaming DataFrames (`df.isStreaming == True`), `write_with_governance` (and its dedicated alias `write_stream_with_governance`) automatically detects strategies requiring micro-batch isolation (`strategy.requires_micro_batch == True`, such as `DeduplicateQuarantineWriteViolationStrategy` and `SplitWriteViolationStrategy`), or when `request.use_micro_batch = True`.

Under the hood:
1. `dc43` wires `df.writeStream.foreachBatch(...)` as the stream sink.
2. For each micro-batch, empty batches are skipped automatically.
3. Non-empty micro-batches execute the violation strategy on the batch slice (`streaming=False`), splitting the batch into clean records and quarantined duplicates.
4. Target tables or paths (resolved automatically from the contract) are pre-created if they do not exist via `ContractDDLBuilder`.
5. Primary records and quarantined duplicates are written via `saveAsTable` or `save` with their respective `valid_writer_modifier` / `reject_writer_modifier` configurations.
6. The stream writer accepts stream-level modifiers via `request.writer_modifier` (e.g. `trigger(availableNow=True)` or `processingTime="10 seconds"`).
7. `execution_result.streaming_queries` holds active `StreamingQuery` handles for monitoring and termination management.

```python
streaming_df = (
    spark.readStream.format("cloudFiles")
    .option("cloudFiles.format", "json")
    .load("/landing/orders")
    .withWatermark("event_time", "10 minutes")
)

request = GovernanceSparkWriteRequest(
    context=GovernanceWriteContext(
        contract=ContractReference(contract_id="sales.orders", version_selector="1.0.0"),
    ),
    options={"checkpointLocation": "/checkpoints/orders_streaming_dedup"},
    violation_strategy=DeduplicateQuarantineWriteViolationStrategy(
        key_properties=["tenant_id", "order_id"],
        sort_column="updated_at",
        keep="first",
        reject_suffix="reject",
    ),
    # Stream-level configuration:
    writer_modifier=lambda writer: writer.trigger(availableNow=True),
)

execution_result = write_stream_with_governance(
    df=streaming_df,
    request=request,
    governance_service=my_governance_client,
)

# Await completion when using availableNow=True
for query in execution_result.streaming_queries:
    query.awaitTermination()
```

---

## Governed Merges (`merge_with_governance`)

For Delta Lake upserts and merges, `merge_with_governance` provides the same contract governance and pre-creation guarantees:

```python
from dc43_integrations.spark.io import merge_with_governance, GovernanceSparkWriteRequest
from dc43_service_clients.governance.models import GovernanceWriteContext, ContractReference

request = GovernanceSparkWriteRequest(
    context=GovernanceWriteContext(
        contract=ContractReference(contract_id="sales.orders", version_selector="1.0.0"),
    ),
    table_properties={"delta.autoOptimize.optimizeWrite": "true"},
)

def configure_merge(builder):
    return (
        builder.whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
    )

exec_result = merge_with_governance(
    source_df=incoming_df,
    condition="target.order_id = source.order_id",
    request=request,
    governance_service=my_governance_client,
    merge_builder_modifier=configure_merge,
)
```

When `merge_with_governance` runs:
1. It validates and casts `incoming_df` against the ODCS contract.
2. If the target Delta table or path (resolved from the contract) does not exist, `ContractDDLBuilder` executes the `CREATE TABLE IF NOT EXISTS` DDL before the merge initiates, preventing Delta Lake missing-table exceptions.
3. The merge executes cleanly on the contract-compliant table.

### Streaming Merges and Triggers (`availableNow`)

`merge_with_governance` (and its alias `merge_stream_with_governance`) natively supports Spark Structured Streaming DataFrames (`source_df.isStreaming == True`). Under the hood, DC43 manages `writeStream.foreachBatch` to execute the Delta Lake upsert on each micro-batch, skipping empty micro-batches and returning active `StreamingQuery` handles in `exec_result.streaming_queries`.

You can configure streaming triggers (such as `availableNow=True` for incremental batch runs, or `processingTime="10 seconds"` for continuous processing) via `writer_modifier`:

```python
from dc43_integrations.spark.io import merge_with_governance, GovernanceSparkWriteRequest
from dc43_service_clients.governance.models import GovernanceWriteContext, ContractReference

streaming_df = spark.readStream.format("cloudFiles").load("/landing/orders")

request = GovernanceSparkWriteRequest(
    context=GovernanceWriteContext(
        contract=ContractReference(contract_id="sales.orders", version_selector="1.0.0"),
    ),
    options={
        "checkpointLocation": "/checkpoints/orders_merge",
    },
    # Trigger availableNow: processes all available new data in micro-batches and then shuts down cleanly
    writer_modifier=lambda writer: writer.trigger(availableNow=True),
    observation_writer_modifier=lambda writer: writer.trigger(availableNow=True),
)

exec_result = merge_with_governance(
    source_df=streaming_df,
    condition="target.order_id = source.order_id",
    request=request,
    governance_service=my_governance_client,
    merge_builder_modifier=lambda b: b.whenMatchedUpdateAll().whenNotMatchedInsertAll(),
)

# Wait for completion (for availableNow=True, this exits once all available data has been merged)
query = exec_result.streaming_queries[0]
query.awaitTermination()
```


### Deduplication & Quarantine in Batch and Streaming Merges

Delta Lake requires that at most one source row matches a given target row during a `MERGE` operation (otherwise Spark raises `DELTA_MULTIPLE_SOURCE_ROW_MATCHING_TARGET_ROW_IN_MERGE`). Furthermore, invalid records violating contract expectations (`NOT NULL`, format, range, or key uniqueness) should be isolated before executing the upsert.

Both `merge_with_governance` and `merge_stream_with_governance` natively support `DeduplicateQuarantineWriteViolationStrategy` and `SplitWriteViolationStrategy`:

1. **Per-batch / micro-batch isolation**: In streaming mode (`foreachBatch`), the violation strategy runs on each micro-batch DataFrame before the Delta merge executes.
2. **Automatic key inference**: `DeduplicateQuarantineWriteViolationStrategy` automatically resolves key columns from the contract (`primaryKey: true` properties or `duplicateValues` rules) when `key_properties` is not explicitly provided.
3. **Dual-sink routing (`MERGE` + Quarantine `APPEND`)**:
   - The **valid / deduplicated subset** is **merged** (`DeltaTable.merge(...)`) into the primary target Delta table using your `condition` and `merge_builder_modifier`.
   - The **rejected / duplicate subset** (`is_reject=True`) is **appended** (`saveAsTable` / `save`) to the quarantine table or path (`<table_or_path>_reject`), which is automatically pre-created with `DDLConstraintPolicy.relaxed()` so malformed or null-key rows never fail Delta DDL invariant checks.
4. **CloudPickle / Spark Connect safe**: The internal `foreachBatch` sink is decoupled from driver-only `SparkSession` and executor state, avoiding `PySparkRuntimeError: [CONTEXT_ONLY_VALID_ON_DRIVER]` on Databricks and Spark Connect.

```python
from dc43_integrations.spark.io import (
    GovernanceSparkWriteRequest,
    merge_stream_with_governance,
)
from dc43_integrations.spark.violation_strategy import (
    DeduplicateQuarantineWriteViolationStrategy,
)
from dc43_service_clients.governance.models import GovernanceWriteContext, ContractReference

request = GovernanceSparkWriteRequest(
    context=GovernanceWriteContext(
        contract=ContractReference(contract_id="sales.orders", version_selector="1.0.0"),
    ),
    options={"checkpointLocation": "/checkpoints/orders_merge"},
    violation_strategy=DeduplicateQuarantineWriteViolationStrategy(
        # key_properties is optional when primaryKey or duplicateValues is defined in the contract
        order_by=[("updated_at", "DESC")],
        reject_suffix="reject",
    ),
    writer_modifier=lambda writer: writer.trigger(availableNow=True),
)

exec_result = merge_stream_with_governance(
    source_df=streaming_df,
    condition="target.order_id = source.order_id",
    request=request,
    governance_service=my_governance_client,
    merge_builder_modifier=lambda b: b.whenMatchedUpdateAll().whenNotMatchedInsertAll(),
)

for query in exec_result.streaming_queries:
    query.awaitTermination()
```
