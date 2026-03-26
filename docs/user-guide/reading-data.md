# Reading Data with Governance

The `dc43_integrations.spark.io` module provides a unified API for reading datasets while enforcing Data Contracts and registering activity with the Data Governance Service. 

## The Core Function: `read_with_governance`

You only need `read_with_governance` (for batch and streaming reads) to ensure that your Spark DataFrames are validated against their registered data contracts.

```python
from dc43_integrations.spark.io import read_with_governance, GovernanceSparkReadRequest
from dc43_integrations.spark.strategy import GovernanceReadContext 
from dc43_service_clients.governance.models import ContractReference, PipelineContext

# 1. Ask the governance layer to resolve the data location via a Contract
request = GovernanceSparkReadRequest(
    context=GovernanceReadContext(
        contract=ContractReference(contract_id="test.orders", version_selector="1.0.0"),
        pipeline_context=PipelineContext(pipeline="daily_ingest"),
    ),
    # If the contract doesn't explicitly define a physical input port, 
    # you can fallback to providing a direct path here:
    # path="s3://lake/orders",
    
    # Optional: Apply contract-reactive DataFrame transformations
    # contract_transformers=["my_custom_module:apply_pii_masking"]
)

# Returns a Spark DataFrame already aligned with the contract schema
df = read_with_governance(
    spark,
    request=request,
    governance_service=my_governance_client,
    enforce=True, # Raise an Error if schema violation occurs
    auto_cast=True, # Attempt safe type casts (e.g. Int -> Long)
)
```

## How It Works

1. **Contract Resolution**: The `governance_service` looks up the `contract_id` and `contract_version` to find the active Data Contract. It automatically resolves the physical input port (the path).
2. **Read Execution**: Spark reads the underlying data from the location specified by the contract.
3. **Transformations**: If `contract_transformers` are defined (globally or passed in the request), they are executed sequentially on the incoming data, allowing for contract-reactive preprocessing.
   * **Sequence Order**: For reads, global defaults (Masking/Restrictions) are executed first, followed by any custom `request.contract_transformers` (User formatting).
4. **Alignment & Observation**: The schema is validated against the resolved contract. If `enforce=True`, missing columns or incompatible types will raise an error (unless `auto_cast` succeeds). 
5. **Governance Reporting**: The integration sends an Observation payload back to the `governance_service`, registering the read event and updating lineage.

> [!TIP]
> **Writing Safe Transformers**
> A `ContractBasedTransformer` takes a DataFrame and an `OpenDataContractStandard` model and returns a DataFrame. Since standard PySpark DataFrame operations (like `.withColumn()`, `.filter()`) run purely on the Driver, they are perfectly safe. However, if your transformer defines a PySpark User-Defined Function (UDF), be careful with Closures! Do not capture the `contract` object inside the UDF itself, as it is a large Pydantic model and may fail serialization when sent to Spark Executors. Instead, extract exactly the primitive values you need *before* defining the UDF.

## Streaming Data

The unified `read_with_governance` function also handles Structured Streaming seamlessly. If your `format` requires streaming (or you pass specific streaming options via `request.options`), the returned DataFrame will be a streaming `DataFrame`. 

Observations are published to governance just like in batch mode, ensuring your catalogue always knows when a stream starts consuming a dataset. 

## Reading Split / Derivative Datasets

If an upstream pipeline writes data using the `SplitWriteViolationStrategy` (routing clean data to a `valid` subset and bad data to a `reject` subset), you must explicitly target the derivative dataset when reading. The contract identity remains exactly the same, but the physical location and dataset identities change.

There are two supported patterns to target these subsets:

### 1. Via Dataset Suffix (Direct Contract Resolution)
Target the base contract, but explicitly bind the read request to the suffixed dataset identifier:

```python
request = GovernanceSparkReadRequest(
    context=GovernanceReadContext(
        contract=ContractReference(contract_id="sales.orders", version_selector="1.0.0"),
        dataset_id="base_dataset_name::valid" # Explicitly read the valid subset
    )
)
```

### 2. Via Data Product Ports (ODPS Standard)
If your organization leverages the Open Data Product Standard, the upstream split strategy usually binds the `valid` and `reject` subsets to distinct output ports. You can read them directly by referencing the data product port:

```python
request = GovernanceSparkReadRequest(
    context=GovernanceReadContext.from_port(
        product="sales_domain.orders_product",
        port="valid" # Target the clean data port
    )
)
```

Targeting split datasets explicitly ensures that your Data Governance catalog accurately maps your lineage to the clean or quarantined derivatives rather than falsely tracing dependencies to the entire unsplit pipeline.
