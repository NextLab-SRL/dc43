# Observations-Driven Contract Drafter

The dc43 contract drafter now operates on runtime observations rather than Spark
specific APIs.  The core helper lives in
`dc43.components.contract_drafter.observations.draft_from_observations` and
expects a schema snapshot plus optional metric payload produced by the data
quality component.  Spark pipelines can capture the schema with
`dc43.components.integration.spark_quality.schema_snapshot` before delegating to
the drafter.

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
from dc43.components.contract_drafter.observations import draft_from_observations
from dc43.components.integration.spark_quality import schema_snapshot

schema = schema_snapshot(dataframe)
metrics = validation_result.metrics

draft = draft_from_observations(
    schema=schema,
    metrics=metrics,
    base_contract=contract,
    bump="minor",
    dataset_id="table:catalog.schema.orders",
    dataset_version="2024-05-30",
    dq_feedback={"status": status.status} if status else None,
)
```

The helper:

* Rebuilds the contract schema based on the observed fields.
* Bumps the semantic version (`major`/`minor`/`patch`).
* Copies IO servers and custom properties, adding provenance metadata and
  optional metric snapshots.
* Returns an ODCS document flagged as `draft` so governance workflows can decide
  whether to promote or reject it.

## Extending the drafter

The drafter is deliberately small so you can augment it with additional signals:

* **Streaming metadata** – enrich the draft with Schema Registry information when
  Structured Streaming hints are available.
* **Batch file introspection** – inspect Parquet/CSV statistics to derive
  nullability or distribution changes when data is loaded from files.
* **Quality feedback** – integrate with the `DQClient` so the drafter can
  summarise outstanding violations inside the draft payload.

When a different runtime platform is responsible for draft generation, implement
the same protocol in a new document under
`docs/implementations/contract-drafter/` and link it from the conceptual
component guide.
