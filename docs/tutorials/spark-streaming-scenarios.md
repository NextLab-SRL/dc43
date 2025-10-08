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

As you trigger each run the demo page now opens a **Live streaming run** card
that listens for server-sent events. The card highlights the active stage,
renders micro-batch badges as they arrive, and appends timeline entries for
validation milestones so observers can follow the pipeline in real time.

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
`demo.streaming.events_processed` contract. The synthetic source emits six
rows per second across a single partition; after roughly eight seconds the
timeline card shows the source heartbeat, the validation pass, and a row of
micro-batch cards that surface the row count, violations, and timestamps for
every processed batch.【F:packages/dc43-demo-app/src/dc43_demo_app/streaming.py†L104-L178】【F:packages/dc43-demo-app/src/dc43_demo_app/templates/pipeline_run_detail.html†L36-L104】

While the run is active the live card increments the batch list and timeline as
soon as the streaming observation writer reports a new micro-batch, allowing
you to narrate how the contract holds even before the final dataset record is
persisted.【F:packages/dc43-demo-app/src/dc43_demo_app/templates/pipeline_run_detail.html†L1-L162】

The dataset history panel now lists a fresh version with status `ok`. Expanding
the DQ details or the timeline reveals the schema snapshot sent to governance
and confirms that no expectations recorded violations.【F:packages/dc43-demo-app/src/dc43_demo_app/streaming.py†L146-L178】

## Scenario 2 – violations trigger reject routing

Running **Streaming: rejects without blocking** flips the sign of every fourth
row. The main write keeps streaming even though validation records violations;
`enforce=False` downgrades the status to `warn` while still publishing metrics
and errors. A secondary streaming write filters the negative rows into the
`demo.streaming.events_rejects` contract so remediation teams can triage them
without losing the rest of the feed.【F:packages/dc43-demo-app/src/dc43_demo_app/streaming.py†L180-L256】

The live progress card differentiates the reject batches, colouring their
badges with the warning palette and calling out the intervention once the
reject sink activates. This makes it easy to point to the exact batch that
introduced the warning status while the stream is still running.【F:packages/dc43-demo-app/src/dc43_demo_app/templates/pipeline_run_detail.html†L1-L162】

After the run completes, the dataset record shows the warning status, the
captured violations, and the reject sink metadata including the number of rows
quarantined for that micro-batch. The timeline panel highlights when the reject
sink activated while the micro-batch cards make it obvious which batches were
rerouted and how many rows were quarantined.【F:packages/dc43-demo-app/src/dc43_demo_app/streaming.py†L180-L256】【F:packages/dc43-demo-app/src/dc43_demo_app/templates/pipeline_run_detail.html†L130-L162】

## Scenario 3 – schema breaks block the stream

The **Streaming: schema break blocks the run** example drops a required column
before writing. Validation fails immediately, no `StreamingQuery` is started,
and the dataset record stores the failure reason without assigning a dataset
version. The timeline and batch list make the failure easy to spot by calling
out the schema violation alongside the captured error message.【F:packages/dc43-demo-app/src/dc43_demo_app/streaming.py†L258-L333】【F:packages/dc43-demo-app/src/dc43_demo_app/templates/pipeline_run_detail.html†L130-L162】

## Driving scenarios from code

The same scenarios are exposed as helpers so you can include them in regression
suites or demonstrations:

```python
from dc43_demo_app.streaming import run_streaming_scenario

run_streaming_scenario("streaming-valid", seconds=8, run_type="observe")
run_streaming_scenario("streaming-dq-rejects", seconds=8, run_type="observe")
run_streaming_scenario("streaming-schema-break", seconds=3, run_type="enforce")
```

Each invocation records a dataset run in the demo workspace, mirroring what the
UI shows after triggering the scenario manually.【F:packages/dc43-demo-app/src/dc43_demo_app/streaming.py†L274-L291】
