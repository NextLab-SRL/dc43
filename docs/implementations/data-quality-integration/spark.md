# Spark Data-Quality Integration

The Spark integration captures schema snapshots and expectation metrics inside
Spark jobs before forwarding them to the **governance service**. Runtime
helpers live in `dc43.services.data_quality.backend.integration` while the
runtime-agnostic evaluation logic stays inside
`dc43.services.data_quality.backend.engine`. Use
`dc43.services.data_quality.backend.validation.apply_contract` to align Spark IO with
an approved contract when reading or writing datasets.

## Helpers

The Spark integration exposes the following building blocks:

* `schema_snapshot(df)` – capture the observed fields as `backend_type`,
  canonical `odcs_type`, and `nullable` flags.
* `collect_observations(df, contract)` – return `(schema, metrics)` tuples ready
  to hand to the governance service via an `ObservationPayload`.
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
  writes (via `dc43.services.data_quality.backend.validation`).

```python
from dc43.services.data_quality import ObservationPayload
from dc43.services.data_quality.backend.integration import build_metrics_payload
from dc43.services.governance.client import build_local_governance_service

metrics_payload, schema_payload, reused = build_metrics_payload(
    df,
    contract,
    validation=None,
    include_schema=True,
)
payload = ObservationPayload(metrics=metrics_payload, schema=schema_payload, reused=reused)
governance = build_local_governance_service(contract_store)
assessment = governance.evaluate_dataset(
    contract_id=contract.id,
    contract_version=contract.version,
    dataset_id="table:catalog.schema.orders",
    dataset_version="2024-05-30",
    validation=None,
    observations=lambda: payload,
    operation="write",
    draft_on_violation=True,
)
status = assessment.status
```

`validate_dataframe` treats schema violations (missing columns, type drift,
required nulls) as blocking failures.  Expectation metrics are downgraded to
warnings by default so pipelines can continue running while governance decides
whether to block.  Pass `expectation_severity="error"` to fail locally on those
violations or `"ignore"` to silence them entirely.

`evaluate_dataset` delegates the final compatibility verdict to whichever
governance backend you configure (filesystem stub, Collibra, bespoke
service). The governance service links datasets to contracts, records
pipeline activity, and generates draft proposals when metrics surface
schema drift.

## Extending the Spark integration

* **Domain-specific metrics** – append calculated KPIs to the metric payload
  before handing it to the governance layer.
* **Observability sinks** – forward schema/metric snapshots to monitoring
  platforms before invoking `GovernanceServiceClient.evaluate_dataset`.
* **Streaming support** – run the helpers inside Structured Streaming
  micro-batches to keep governance dashboards up to date.

If you maintain a different execution engine—such as Soda Core, Great
Expectations, or a SQL warehouse—mirror this document in
`docs/implementations/data-quality-integration/` and describe how observations
reach `evaluate_contract` in that environment.
