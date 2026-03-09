# Handling Violations

When `dc43-integrations` assesses a DataFrame against a contract, it produces a deep `ValidationResult` containing expectation metrics, schema errors, and the final status of the dataset. You control how aggressive the framework is via policies and argument flags.

## Enforcement Modes

By default, passing `enforce=True` into `write_with_governance` or `read_with_governance` makes the integration strictly block processing on any errors:
- If a column is missing from the contract, an Exception is raised.
- If data quality expectations fail out-of-bounds, an Exception is raised.

When you pass `enforce=False`, the execution completes successfully and allows the Spark job to continue, *even if* violations occurred. The violations are still submitted to the Data Governance Service for catalog tracking.

## Governance Policies

The `GovernanceWriteContext` allows you to inject `GovernancePolicy` configurations. These define how the Governance Service reacts to drift:

```python
policy = GovernancePolicy(
    draft_on_violation=True,       # Automatically create a draft contract version when drift is detected
    fail_on_breaking_schema=False, # Ignore breaking schema changes initially, to allow staging 
)
```

If `draft_on_violation=True`, when the framework detects a new column injected by a data producer, it will automatically propose a "draft" bump of the contract and notify data stewards for review, instead of just flatly rejecting the pipeline.

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
