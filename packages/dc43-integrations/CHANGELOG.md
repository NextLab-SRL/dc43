# dc43-integrations changelog

## [Unreleased]
### Added
- Added explicit streaming read/write helpers (``read_stream_with_contract``,
  ``read_stream_from_contract``, ``read_stream_from_data_product``,
  ``write_stream_with_contract``, and ``write_stream_to_data_product``) so
  Structured Streaming jobs can enforce contracts via
  ``readStream``/``writeStream`` while still invoking the data-quality service
  and governance catalogue (metrics are deferred but schema observations
  continue to flow). Documentation now demonstrates capturing the resulting
  ``StreamingQuery`` handles and the integration tests cover both read and
  write pipelines.
- Streaming validations now surface the ``dataset_id`` and ``dataset_version``
  submitted to governance so micro-batch monitors can inspect the active
  snapshot and request asynchronous metric computation when necessary.
- Streaming writes launch an auxiliary ``foreachBatch`` metrics collector so
  contract expectations compute Spark metrics per micro-batch, feed the
  data-quality service, and update the returned validation payloads while the
  primary sink continues to run.
- Streaming validation payloads now expose a ``streaming_batches`` timeline so
  callers can inspect per-batch row counts, violation totals, timestamps, and
  intervention reasons alongside the aggregated metrics.
- Streaming write helpers accept an ``on_streaming_batch`` callback that emits
  the same payloads recorded in ``streaming_batches`` so applications can stream
  live progress updates or power custom dashboards while the pipeline runs.
- Added a streaming scenarios walkthrough and linked demo-application flows that
  demonstrate continuous validation, reject routing, and schema break handling
  for Structured Streaming pipelines.
- Streaming writes now rely on a dedicated ``StreamingObservationWriter`` that
  avoids shared-state locking, exposes optional ``StreamingInterventionStrategy``
  hooks to block or reroute pipelines after repeated issues, and ships
  convenience wrappers (``read_stream_with_contract`` /
  ``write_stream_with_contract``) to separate batch and streaming flows in
  caller code.
- Enforced Open Data Contract status guardrails in the Spark read/write helpers with
  configurable policies that default to rejecting non-active contracts and expose
  overrides through the existing read and write strategies.
- Documented the new contract status options across the demo pipeline, integration
  helper, and docs to help teams opt into draft or deprecated contracts for
  development scenarios while keeping production defaults strict.
- Added contract- and data-product-specific helpers
  (`read_from_contract`, `write_with_contract_id`, `read_from_data_product`,
  `write_to_data_product`) that resolve ODPS contracts automatically and abort
  pipelines when port registration produces a draft version.
- `read_from_data_product` accepts an `expected_contract_version` argument so
  data-product reads can pin the upstream schema revision.

### Removed
- Removed the Databricks `UnityCatalogPublisher` wrapper now that Unity Catalog
  tagging is handled transparently by the governance backend configuration.
- Removed the standalone `streaming_contract_scenarios.py` example script; the
  demo application now hosts the streaming walkthrough.

### Fixed
- Installing the ``test`` extra now pulls in ``dc43-service-backends`` with its
  SQL dependencies and ``httpx`` so the integration suite runs without tweaking
  import guards.
- `StrictWriteViolationStrategy` now reuses the wrapped strategy's contract status
  allowances so strict enforcement respects custom governance policies.
- Spark writes that overwrite their source path now checkpoint the aligned dataframe
  before executing so contract-enforced data product registrations succeed without
  triggering spurious "file not found" failures, and `write_to_data_product`
  accepts explicit contract identifiers for the final stage of DP pipelines.
- Streaming observation writers skip empty micro-batches instead of overwriting
  the previous validation payload, so governance metrics (e.g., `row_count`) and
  intervention reasons remain visible after queries drain or stop.
- Filtered missing metric warnings for streaming reads when metrics collection
  is deferred, keeping schema snapshots visible without spurious alerts.
