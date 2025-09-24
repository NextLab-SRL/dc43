# Spark Observations Helper

The Spark integration gathers schema snapshots and contract-driven metrics so the
pure-Python dc43 data-quality engine can evaluate them.  Runtime helpers live in
`dc43.components.integration.spark_quality` and coordinate with the generic
`dc43.components.data_quality.engine.evaluate_contract` function to derive
validation results.

## Helpers

The module exposes the following building blocks:

* `schema_snapshot(df)` — capture the observed fields as `backend_type`,
  canonical `odcs_type`, and `nullable` flags.
* `collect_observations(df, contract)` — return `(schema, metrics)` tuples ready
  to send to the engine.
* `validate_dataframe(df, contract)` — collect observations and pass them to
  `evaluate_contract`, yielding a `ValidationResult` with cached schema/metrics.
* `build_metrics_payload(df, contract, validation=...)` — reuse cached metrics or
  compute fresh ones before submitting them to a governance adapter.
* `attach_failed_expectations(df, contract, status, collect_examples)` — enrich a
  governance `DQStatus` with failing expressions and optional sample rows.
* `apply_contract(df, contract)` — align column order and types before reads and
  writes.

```python
from dc43.components.integration.spark_quality import (
    collect_observations,
    validate_dataframe,
    build_metrics_payload,
)
from dc43.components.data_quality.engine import evaluate_contract
from dc43.components.data_quality.governance import DQClient

schema, metrics = collect_observations(df, contract)
result = evaluate_contract(contract, schema=schema, metrics=metrics)

metrics_payload, _, reused = build_metrics_payload(df, contract, validation=result)
if reused:
    print("Used cached metrics")
else:
    print("Computed metrics after validation")
status = dq_client.submit_metrics(
    contract=contract,
    dataset_id="table:catalog.schema.orders",
    dataset_version="2024-05-30",
    metrics=metrics_payload,
)
```

`submit_metrics` delegates the final compatibility verdict to whichever data
quality governance adapter you configure (filesystem stub, Collibra, bespoke
service).  The Spark helpers concern themselves with collecting evidence from
runtime dataframes so the engine and governance layers can remain runtime-agnostic.

## Extending the Spark integration

* **Domain-specific metrics** – append calculated KPIs to the metric payload
  before handing it to the governance layer.
* **Observability sinks** – forward schema/metric snapshots to monitoring
  platforms before invoking `DQClient.submit_metrics`.
* **Streaming support** – run the helpers inside Structured Streaming
  micro-batches to keep governance dashboards up to date.

If you maintain a different execution engine—such as Soda Core, Great
Expectations, or a SQL warehouse—mirror this document in
`docs/implementations/data-quality-engine/` and describe how observations reach
`evaluate_contract` in that environment.
