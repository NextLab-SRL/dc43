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
    # path="s3://lake/orders"
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
3. **Alignment & Observation**: The schema is validated against the resolved contract. If `enforce=True`, missing columns or incompatible types will raise an error (unless `auto_cast` succeeds). 
4. **Governance Reporting**: The integration sends an Observation payload back to the `governance_service`, registering the read event and updating lineage.

## Streaming Data

The unified `read_with_governance` function also handles Structured Streaming seamlessly. If your `format` requires streaming (or you pass specific streaming options via `request.options`), the returned DataFrame will be a streaming `DataFrame`. 

Observations are published to governance just like in batch mode, ensuring your catalogue always knows when a stream starts consuming a dataset. 
