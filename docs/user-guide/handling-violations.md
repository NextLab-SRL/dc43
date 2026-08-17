# Handling Violations

When `dc43-integrations` assesses a DataFrame against a contract, it produces a deep `ValidationResult` containing expectation metrics, schema errors, and the final status of the dataset. You control how aggressive the framework is via policies and argument flags.

## Dual-Layer Enforcement Model

`dc43` separates structural guarantees from data-level content validation:

1. **Hard Gating (Structural DDL & Schema Integrity - Unconditional)**:
   - Physical table structure, column types, nullability (`NOT NULL`), Primary Keys (`PRIMARY KEY`), and partitioning (`PARTITIONED BY` / `CLUSTER BY`) are **strictly gated**.
   - Setting `enforce=False` **never** bypasses structural DDL generation. When a table is created, it is guaranteed to match the contract DDL, preventing misaligned jobs from creating corrupted table schemas in shared catalogs.
2. **Soft Gating (Data Quality & Metric Thresholds - Conditional)**:
   - Metric rules (value bounds, regex, null ratios) are controlled by the `enforce` flag and violation strategies:
     - `enforce=True`: Strictly blocks processing and raises an Exception if data quality bounds are breached or if a post-write governance interceptor fails.
     - `enforce=False`: Allows the Spark job to continue writing data, while submitting the violations and observations to the Data Governance Service for catalog tracking and lineage audit.

## Governance Policies

The `GovernanceWriteContext` allows you to inject `GovernancePolicy` configurations. These define how the Governance Service reacts to drift:

```python
policy = GovernancePolicy(
    draft_on_violation=True,       # Automatically create a draft contract version when drift is detected
    fail_on_breaking_schema=False, # Ignore breaking schema changes initially, to allow staging 
)
```

If `draft_on_violation=True`, when the framework detects a new column injected by a data producer, it will automatically propose a "draft" bump of the contract and notify data stewards for review, instead of just flatly rejecting the pipeline.

## Advanced Write Strategies (Splitting Data)

Contract enforcement often needs to react differently depending on the severity of the issues or the downstream consumer. Instead of a simple pass/fail, you can pass a `violation_strategy` to `write_with_governance` to orchestrate advanced remediation:

- **`SplitWriteViolationStrategy`**: Filters the aligned rows based on the data-quality expectations. It automatically splits the data and creates two derivative datasets: a `valid` subset (clean records) and a `reject` subset (bad records). Data stewards can triage the discarded records without blocking the entire run from flowing downstream.
- **`StrictWriteViolationStrategy`**: Wraps another strategy (like split) and forcefully flips the final validation result to `ok=False` if any violations occurred, ensuring that job orchestrators (like Airflow or Databricks) mark the run as failed, even though the data was correctly written and split into remediation queues.

```python
from dc43_integrations.spark.violation_strategy import SplitWriteViolationStrategy, StrictWriteViolationStrategy

# Inside write_with_governance
violation_strategy=StrictWriteViolationStrategy(
    strategy=SplitWriteViolationStrategy(
        valid_suffix="valid",
        reject_suffix="reject"
    )
)
```

> [!NOTE]
> When using a split strategy, the integration layer automatically reports the `valid` and `reject` subsets as distinct pipeline observations back to the Governance Service. Ensure these derivative datasets are correctly registered in your Data Product definitions or Metadata Catalog.

## Streaming Intervention

For streaming queries, continuous micro-batch failures require strategic interventions to prevent data poisoning or silent outages.

```python
from dc43_integrations.spark.io.streaming import StreamingInterventionStrategy, StreamingInterventionContext

class RejectSinkInterventionStrategy(StreamingInterventionStrategy):
    def decide(self, context: StreamingInterventionContext) -> Optional[str]:
        if not context.validation.ok:
            return "Block: Data quality constraints vastly exceeded."
        return None

# Passed inside `write_with_governance`
streaming_intervention_strategy=RejectSinkInterventionStrategy()
```

If the strategy's `decide()` returns a string, the framework throws a `StreamingInterventionError` halting the stream to prevent dirty data from filling your sink, storing the `reason` in the stream's validation details.
