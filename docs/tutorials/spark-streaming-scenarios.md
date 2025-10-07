# Spark streaming scenarios in the demo app

The dc43 demo application now bundles a trio of Structured Streaming examples
that exercise the contract-aware read/write helpers. Each scenario aligns with a
common operations story:

* **Healthy ingestion** – all micro-batches pass validation and the registry
  records a governed dataset version for every slice.
* **Reject routing** – contract violations are surfaced but the pipeline keeps
  running while a secondary sink quarantines the failing rows.
* **Schema drift** – a breaking change is detected before materialisation so no
  dataset version is published.

The walkthrough below explains how to explore the scenarios from the UI and how
engineers can drive them programmatically when writing tests or running live
workshops.

## Prerequisites

1. Install the demo application and its streaming dependencies:

   ```bash
   pip install pyspark==3.5.1 httpx
   pip install -e .
   ```

2. Launch the demo stack. This starts the contracts backend, the contracts UI,
   and the pipeline demo server in a single process:

   ```bash
   python -m dc43_demo_app.runner
   ```

3. Open the pipeline demo (defaults to http://127.0.0.1:8000) and navigate to
   the **Streaming pipelines** section on the “Pipeline scenarios” page.

## Scenario 1 – healthy batches remain governed

Select **Streaming: healthy pipeline** and run the scenario. The demo app uses
`read_stream_with_contract` to attach governance to Spark’s `rate` source and
`write_stream_with_contract` to persist the aligned stream under the
`demo.streaming.events_processed` contract. Validation stays enabled for every
micro-batch, the observation writer collects metrics such as `row_count`, and
the dataset record highlights the governed version alongside the active
`StreamingQuery` metadata.【F:packages/dc43-demo-app/src/dc43_demo_app/streaming.py†L102-L156】【F:packages/dc43-demo-app/src/dc43_demo_app/scenarios.py†L2004-L2033】

The dataset history panel now lists a fresh version with status `ok`. Expanding
the DQ details reveals the metrics captured for the latest batch and the schema
snapshot sent to governance.【F:packages/dc43-demo-app/src/dc43_demo_app/streaming.py†L120-L154】

## Scenario 2 – violations trigger reject routing

Running **Streaming: rejects without blocking** flips the sign of every fourth
row. The main write keeps streaming even though validation records violations;
`enforce=False` downgrades the status to `warn` while still publishing metrics
and errors. A secondary streaming write filters the negative rows into the
`demo.streaming.events_rejects` contract so remediation teams can triage them
without losing the rest of the feed.【F:packages/dc43-demo-app/src/dc43_demo_app/streaming.py†L158-L215】

After the run completes, the dataset record shows the warning status, the
captured violations, and the reject sink metadata including the number of rows
quarantined for that micro-batch.【F:packages/dc43-demo-app/src/dc43_demo_app/streaming.py†L205-L215】

## Scenario 3 – schema breaks block the stream

The **Streaming: schema break blocks the run** example drops a required column
before writing. Validation fails immediately, no `StreamingQuery` is started,
and the dataset record stores the failure reason without assigning a dataset
version. This mirrors the production behaviour where enforcement keeps the
catalogue consistent with the actual materialised data.【F:packages/dc43-demo-app/src/dc43_demo_app/streaming.py†L217-L272】

## Driving scenarios from code

The same scenarios are exposed as helpers so you can include them in regression
suites or demonstrations:

```python
from dc43_demo_app.streaming import run_streaming_scenario

run_streaming_scenario("streaming-valid", seconds=5, run_type="observe")
run_streaming_scenario("streaming-dq-rejects", seconds=5, run_type="observe")
run_streaming_scenario("streaming-schema-break", seconds=3, run_type="enforce")
```

Each invocation records a dataset run in the demo workspace, mirroring what the
UI shows after triggering the scenario manually.【F:packages/dc43-demo-app/src/dc43_demo_app/streaming.py†L274-L291】
