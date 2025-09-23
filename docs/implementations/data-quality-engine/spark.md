# Spark Data Quality Engine

The Spark implementation of the dc43 data-quality (DQ) engine translates
ODCS expectations into Spark jobs that validate schema compatibility,
compute metrics, and attach failure context. The helpers live in
`dc43.components.data_quality.engine.spark` and integrate with
`write_with_contract` / `read_with_contract` to feed governance systems
with live observations.

## Helpers

The module exposes core helpers:

* `validate_dataframe(df, contract)` — runs schema validation and metric
  computation, returning a `ValidationResult` that includes errors,
  warnings, metrics, and a schema snapshot.
* `schema_snapshot(df)` / `spark_type_name(type_hint)` — utility helpers
  used by integration layers when aligning DataFrames to contracts.
* `expectations_from_contract(contract)` — returns SQL predicates for
  field-level expectations (used internally by the validation and metric
  routines).
* `compute_metrics(df, contract)` — executes the predicates plus
  uniqueness checks and schema-level queries.
* `attach_failed_expectations(df, contract, status, collect_examples)` —
  enriches a `DQStatus` with failed expectations and optional sample
  rows.

```python
from dc43.components.data_quality.engine.spark import (
    attach_failed_expectations,
    validate_dataframe,
)
from dc43.components.data_quality.governance import DQClient

dq_client: DQClient = ...
result = validate_dataframe(df, contract)
status = dq_client.submit_metrics(
    contract=contract,
    dataset_id="table:catalog.schema.orders",
    dataset_version="2024-05-30",
    metrics=result.metrics,
)
status.details = status.details or {}
status.details["schema"] = result.schema
status = attach_failed_expectations(
    df,
    contract,
    status,
    collect_examples=True,
)
```

`submit_metrics` delegates the final compatibility verdict to whichever DQ
governance adapter you configure (filesystem stub, Collibra, bespoke service).
The Spark engine concerns itself with collecting schema evidence, metrics,
and surfacing failure context.

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
