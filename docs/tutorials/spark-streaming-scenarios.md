# Spark streaming contract scenarios

The streaming helpers introduced in `dc43_integrations.spark.io` make it possible to
validate incoming micro-batches, forward observations to governance, and keep
running even when a pipeline has to quarantine bad records.  The examples in
`packages/dc43-integrations/examples/streaming_contract_scenarios.py` wire those
pieces together and showcase three common situations for contract-first
pipelines.

## Prerequisites

1. Install the runtime dependencies:

   ```bash
   pip install pyspark==3.5.1 httpx
   pip install -e .
   ```

2. Launch any of the scenarios from the repository root.  Each run builds a
   local Spark session, writes the contract to a temporary filesystem store, and
   prints the validation outcome together with sample records.

   ```bash
   python packages/dc43-integrations/examples/streaming_contract_scenarios.py <scenario>
   ```

   Replace `<scenario>` with one of the options described below (`valid`,
   `dq-rejects`, `schema-break`, or `all`).  Use `--seconds` to control how long
   the helper drives micro-batches for each scenario.

## Scenario 1 – continuous validation of healthy batches

The `valid` scenario connects to Spark's `rate` source through
`read_stream_with_contract` and writes the aligned stream to the in-memory sink
with `write_stream_with_contract`.  Every micro-batch is checked against the
contract's expectation plan (positive, non-null values) and the observation
writer forwards metrics such as `row_count` and `violations.non_negative_value`
back to the data-quality client.【F:packages/dc43-integrations/examples/streaming_contract_scenarios.py†L261-L307】【5282a1†L1-L31】

Sample output (truncated) shows the dataset version captured during the read, a
series of metric snapshots, and the latest validation details:

```
=== Valid streaming pipeline ===
read dataset version: demo.rate_stream@2025-10-07T11:37:18.762882Z
recorded micro-batches:
  batch 3: metrics={'row_count': 24, 'violations.non_negative_value': 0, ...}
...
latest validation: {'ok': True, 'dataset_version': '2025-10-07T11:37:19.078593Z',
                    'streaming_metrics': {'row_count': 4, ...}}
```

After the queries drain, the helper prints the head of the in-memory sink so you
can confirm that the job continues to process events while staying compliant
with the contract schema.【5282a1†L1-L33】

## Scenario 2 – continue processing while routing rejects

The `dq-rejects` scenario applies a transformation that inverts every fourth
value before writing the stream.  Contract validation keeps running in
streaming mode, but the write helper is invoked with `enforce=False` so
violations turn into observable errors instead of stopping the pipeline.  A
second streaming query filters negative values into a dedicated reject sink
while the primary sink continues to receive the full dataset.【F:packages/dc43-integrations/examples/streaming_contract_scenarios.py†L309-L372】

As the observation writer evaluates each micro-batch, the data-quality stub logs
metrics such as `violations.non_negative_value` and retains the latest
validation payload.  The printed summary highlights the accumulated errors and
reports how many rows were captured in the reject table alongside an excerpt of
those rejected records.【F:packages/dc43-integrations/examples/streaming_contract_scenarios.py†L352-L372】

## Scenario 3 – halt on schema break

The `schema-break` scenario drops the `value` column before calling
`write_stream_with_contract`.  Because the expectation plan requires non-null
values, the first micro-batch triggers a validation failure.  With the default
`enforce=True`, the observation writer raises the validation error, the
streaming query terminates, and the summary prints the failing metrics so the
pipeline can be paused or rerouted.【F:packages/dc43-integrations/examples/streaming_contract_scenarios.py†L374-L434】

Running all three scenarios (`--seconds` can be reduced for quicker demos)
provides a reproducible walkthrough of how contract-aware streaming reads and
writes behave when everything is healthy, when quality issues should be routed
elsewhere, and when the schema drifts in a breaking way.
