# Spark Data-Quality Integration

The Spark integration captures schema snapshots and expectation metrics inside
Spark jobs before forwarding them to the data-quality governance layer. Runtime
helpers live in `dc43.components.data_quality.integration` while the
runtime-agnostic evaluation logic stays inside
`dc43.components.data_quality.engine`. Use
`dc43.components.data_quality.validation.apply_contract` to align Spark IO with
an approved contract when reading or writing datasets.

## Helpers

The Spark integration exposes the following building blocks:

* `schema_snapshot(df)` – capture the observed fields as `backend_type`,
  canonical `odcs_type`, and `nullable` flags.
* `collect_observations(df, contract)` – return `(schema, metrics)` tuples ready
  to send to the governance interface.
* `validate_dataframe(df, contract)` – optional helper that runs the collected
  observations through the engine locally to produce a `ValidationResult` with
  cached schema and metrics.
* `build_metrics_payload(df, contract, validation=...)` – reuse cached metrics or
  compute fresh ones before submitting them to a governance adapter.
* `expectations_from_contract(contract)` – expose Spark SQL predicates matching
  the contract expectations (useful for DLT pipelines).
* `attach_failed_expectations(contract, status)` – enrich a governance
  `DQStatus` with failing expressions and violation counts after a submission.
* `apply_contract(df, contract)` – align column order and types before reads and
  writes (via `dc43.components.data_quality.validation`).

```python
from dc43.components.data_quality import build_metrics_payload, schema_snapshot
from dc43.components.data_quality.integration import collect_observations
from dc43.components.data_quality.governance import DQClient

schema, metrics = collect_observations(df, contract)
metrics_payload, _, _ = build_metrics_payload(
    df,
    contract,
    validation=None,
    include_schema=True,
)
status = dq_client.submit_metrics(
    contract=contract,
    dataset_id="table:catalog.schema.orders",
    dataset_version="2024-05-30",
    metrics=metrics_payload,
)
```

`validate_dataframe` treats schema violations (missing columns, type drift,
required nulls) as blocking failures.  Expectation metrics are downgraded to
warnings by default so pipelines can continue running while governance decides
whether to block.  Pass `expectation_severity="error"` to fail locally on those
violations or `"ignore"` to silence them entirely.

`submit_metrics` delegates the final compatibility verdict to whichever
data-quality governance adapter you configure (filesystem stub, Collibra,
bespoke service).  Those adapters reuse the engine to evaluate the submitted
observations so the integration and engine layers stay decoupled.

## Extending the Spark integration

* **Domain-specific metrics** – append calculated KPIs to the metric payload
  before handing it to the governance layer.
* **Observability sinks** – forward schema/metric snapshots to monitoring
  platforms before invoking `DQClient.submit_metrics`.
* **Streaming support** – run the helpers inside Structured Streaming
  micro-batches to keep governance dashboards up to date.

If you maintain a different execution engine—such as Soda Core, Great
Expectations, or a SQL warehouse—mirror this document in
`docs/implementations/data-quality-integration/` and describe how observations
reach `evaluate_contract` in that environment.
