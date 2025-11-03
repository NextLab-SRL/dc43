# Observations-Driven Contract Drafter

The dc43 contract drafter operates on runtime-agnostic observations. The
core helper lives in
`dc43_service_backends.contracts.backend.drafting.draft_from_observations`
and expects a schema snapshot produced by the data-quality engine. Use
`dc43_integrations.spark.data_quality.schema_snapshot` (or an
equivalent helper in your runtime) to gather the fields before delegating
to the drafter. The Spark package also exposes the convenience helper
`dc43_integrations.spark.contracts.draft_contract_from_dataframe` which
wraps the snapshotting and draft orchestration for DataFrames.

## Inputs and context

The helper consumes three categories of inputs:

1. **Observed schema** – a mapping created by `schema_snapshot(df)` where each
   field exposes an `odcs_type`, `backend_type`, and `nullable` flag.
2. **Approved contract** – the last validated ODCS contract retrieved from the
   contract store.
3. **Operational context** – identifiers that describe the dataset version
   (`dataset_id`, `dataset_version`) and, optionally, the latest data-quality
   verdict.  Supplying the data-quality status gives the drafter awareness of
   recent enforcement failures so it can annotate the draft accordingly.  When
   metric snapshots are available they are stored in the draft's custom
   properties for steward review.

## Usage

```python
from dc43_integrations.spark.contracts import draft_contract_from_dataframe

result = draft_contract_from_dataframe(
    dataframe,
    base_contract=contract,
    dataset_id="table:catalog.schema.orders",
    dataset_version="2024-05-30",
    collect_metrics=True,
)

draft = result.contract
observed_schema = result.schema
observed_metrics = result.metrics
```

The helper:

* Rebuilds the contract schema based on the observed fields.
* Bumps the semantic version (`major`/`minor`/`patch`).
* Copies IO servers and custom properties, adding provenance metadata and
  optional metric snapshots when `collect_metrics=True`.
* Returns an ODCS document flagged as `draft` so governance workflows can decide
  whether to promote or reject it.

## Extending the drafter

The drafter is deliberately small so you can augment it with additional signals:

* **Streaming metadata** – enrich the draft with Schema Registry information when
  Structured Streaming hints are available.
* **Batch file introspection** – inspect Parquet/CSV statistics to derive
  nullability or distribution changes when data is loaded from files.
* **Quality feedback** – integrate with the governance service so the drafter
  can summarise outstanding violations inside the draft payload.

When a different runtime platform is responsible for draft generation, implement
the same protocol in a new document under
`docs/implementations/contract-drafter/` and link it from the conceptual
component guide.
