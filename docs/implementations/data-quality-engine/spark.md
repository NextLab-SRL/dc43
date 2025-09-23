# Spark Data Quality Engine

The Spark implementation of the dc43 data-quality (DQ) engine translates
ODCS expectations into Spark jobs that compute metrics and attach failure
context. The helpers live in `dc43.dq.engine.spark` and integrate with
`write_with_contract` / `read_with_contract` to feed governance systems
with live observations.

## Helpers

The module exposes three primary helpers:

* `expectations_from_contract(contract)` — returns SQL predicates for
  field-level expectations.
* `compute_metrics(df, contract)` — executes the predicates plus
  uniqueness checks and schema-level queries.
* `attach_failed_expectations(df, contract, status, collect_examples)` —
  enriches a `DQStatus` with failed expectations and optional sample
  rows.

```python
from dc43.dq.engine.spark import compute_metrics, attach_failed_expectations

metrics = compute_metrics(df, contract)
status = dq_client.submit_metrics(
    contract=contract,
    dataset_id="table:catalog.schema.orders",
    dataset_version="2024-05-30",
    metrics=metrics,
)
status = attach_failed_expectations(
    df,
    contract,
    status,
    collect_examples=True,
)
```

## Extending the Spark engine

* **Domain-specific metrics** – append calculated KPIs to the returned
  dictionary (for example `metrics["domain.on_time_shipment"]`).
* **Observability sinks** – forward the metrics to monitoring platforms
  before invoking `DQClient.submit_metrics`.
* **Streaming support** – run the helpers inside Structured Streaming
  micro-batches to keep governance dashboards up to date.

If you maintain a different execution engine—such as Soda Core, Great
Expectations, or a SQL warehouse—mirror this document in
`docs/implementations/data-quality-engine/` and describe how the same
protocol is satisfied there.
