# Spark Contract Drafter Helper

The Spark implementation of the dc43 contract drafter turns observed
Spark schemas and runtime feedback into Open Data Contract Standard
(ODCS) draft documents. It is built around
`dc43.components.contract_drafter.spark.draft_from_dataframe` and is typically invoked by
`write_with_contract(..., draft_on_mismatch=True)` whenever a job writes
data that diverges from the approved contract.

## Inputs and context

The helper consumes three categories of inputs:

1. **Observed schema** – the Spark `DataFrame` representing the dataset
   version being produced.
2. **Approved contract** – the last validated ODCS contract retrieved
   from the contract store.
3. **Operational context** – identifiers that describe the dataset
   version (`dataset_id`, `dataset_version`) and, optionally, the latest
   data-quality verdict. Supplying the data-quality status gives the
   drafter awareness of recent enforcement failures so it can annotate
   the draft accordingly.

By combining the schema with the governance context, the drafter can
highlight whether a proposed change is a remediation of a known
incompatibility or a completely new field.

## Usage

```python
from dc43.components.contract_drafter.spark import draft_from_dataframe
from dc43.components.integration.spark_io import write_with_contract

draft = draft_from_dataframe(
    df=dataframe,
    base_contract=contract,
    bump="minor",
    dataset_id="table:catalog.schema.orders",
    dataset_version="2024-05-30",
    dq_status=status,  # optional DQStatus instance
)
```

The helper:

* Rebuilds the contract schema based on the DataFrame.
* Bumps the semantic version (`major`/`minor`/`patch`).
* Copies IO servers and custom properties, adding provenance metadata
  and data-quality context.
* Returns an ODCS document flagged as `Draft` so governance workflows can
  decide whether to promote or reject it.

## Extending the Spark drafter

The Spark implementation is deliberately small so you can augment it
with additional signals:

* **Streaming metadata** – enrich the draft with Schema Registry
  information when Structured Streaming hints are available.
* **Batch file introspection** – inspect Parquet/CSV statistics to derive
  nullability or distribution changes when data is loaded from files.
* **Quality feedback** – integrate with the `DQClient` so the drafter can
  summarise outstanding violations inside the draft payload.

When a different runtime platform is responsible for draft generation,
implement the same protocol in a new document under
`docs/implementations/contract-drafter/` and link it from the
conceptual component guide.
