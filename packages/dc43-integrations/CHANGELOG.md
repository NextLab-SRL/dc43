# dc43-integrations changelog

## [Unreleased]

### Added
- Added `generate_contract_dataset` testing helper to materialise Faker-powered
  sample datasets aligned with ODCS contracts and write them to the configured
  storage path for integration tests.
- `generate_contract_dataset` now wires governance orchestration through a
  provided or inline governance client instead of requesting contract and data
  quality services directly, simplifying integration overrides.

### Changed
- Spark read/write helpers can now operate with only a governance client. When
  provided, the governance service resolves contracts, describes expectations,
  and evaluates data-quality observations without requiring separate contract or
  data-quality clients, reducing the boilerplate for notebooks and pipelines.
- Added `read_with_governance`/`write_with_governance` wrappers (and streaming
  counterparts) plus refreshed docs/tests so common flows just pass a governance
  client loaded from configuration.
- `read_with_governance` and `read_stream_with_governance` now accept
  `GovernanceReadContext` payloads to capture contract references or
  data-product input bindings directly when resolving datasets through the
  governance client.
- Introduced `GovernanceSparkReadRequest`/`GovernanceSparkWriteRequest` so the
  governance wrappers (batch and streaming) collapse their signatures down to a
  governance client plus a single orchestration payload describing contract
  references, data product bindings, dataset locators, and Spark-specific
  overrides.
- Reordered the governance helper signatures so requests sit directly after the
  Spark/DataFrame argument and the governance client follows, matching the
  expected call flow in documentation and tests.
- Delta Live Tables decorators (`governed_table`, `governed_view`,
  `governed_expectations`) now accept governance read contexts and resolve
  expectation plans through the governance service so pipelines initialise only
  the governance client when binding contracts.
- Documented that the DLT annotations depend solely on the governance client,
  matching the usage expectations established by the Spark governance wrappers.
- Updated the Spark setup bundle and generated pipeline stubs to call the
  governance-only read/write helpers and emit `GovernanceSparkReadRequest`/
  `GovernanceSparkWriteRequest` payloads, with accompanying guide updates for
  Databricks, remote Spark, and the contracts app integration helper.

## [0.22.0.0] - 2025-10-25
### Changed
- No functional updates landed for this distribution. Metadata is bumped for the
  0.22.0.0 release rollout.

## [0.21.0.0] - 2025-10-23
### Added
- Spark and Delta Live Tables setup-bundle providers now export complete
  example projects (pipeline modules, README files, and operational helpers)
  so the wizard's archive contains runnable scaffolds for each integration.
- Published setup bundle pipeline stub providers for Spark and Delta Live
  Tables so the setup wizard can assemble integration-specific helper scripts
  directly from the runtime packages.
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
- Local DLT helpers now expose ``ensure_dlt_module`` and ship an in-repo stub so
  demo pipelines and tests keep running even when the ``databricks-dlt`` wheel
  is not available (still recommending the official package for parity).

### Removed
- Removed the Databricks `UnityCatalogPublisher` wrapper now that Unity Catalog
  tagging is handled transparently by the governance backend configuration.
- Removed the standalone `streaming_contract_scenarios.py` example script; the
  demo application now hosts the streaming walkthrough.

### Fixed
- Installing the ``test`` extra now pulls in ``databricks-dlt`` so the local DLT
  harness tests execute instead of being skipped when the dependency is absent.
- The CI workflow now installs the repository's ``dc43-service-backends``
  package before resolving the ``test`` extra so dependency checks continue to
  target ``0.21.0.0`` while avoiding missing-distribution errors during
  integration runs.
- Installing the ``test`` extra now pulls in ``dc43-service-backends`` with its
  SQL dependencies and ``httpx`` so the integration suite runs without tweaking
  import guards.
- Declared a direct dependency on ``dc43-service-backends`` so Spark helpers
  can import the shared ODCS utilities without relying on the meta package.
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
