# dc43-integrations changelog

## [Unreleased]

### Added
- Added `draft_contract_from_dataframe` to capture schema/metric observations
  from Spark DataFrames and return ready-to-review ODCS draft contracts using
  the shared builders from the new `dc43-core` package.
- Added a Databricks Delta versioning notebook that generates evolving
  contracts, writes governed tables, and prints the compatibility matrix for
  quick validation of governance behaviour.
- Added a Databricks Delta streaming notebook that executes Structured Streaming
  runs under evolving contracts and prints the governance compatibility matrix
  after each append.
- Added Delta Live Tables notebook variants so pipelines can exercise the same
  governed versioning walkthrough without adapting the Spark jobs manually.

### Changed
- Expanded the Spark/Databricks integration guide with governance request
  payloads, dataset locator strategies, and violation-handling examples so
  pipelines can reuse the documented patterns directly.
- Clarified the Spark governance payload options (`pipeline_context`,
  `publication_mode`) and the split-write example parameters so callers know
  how suffixes alter table names, dataset identifiers, and emitted lineage
  metadata.
- Added guidance for Spark read status strategies so contract readiness checks
  and data-product policies mirror the documented write-strategy controls.
- Clarified that data-product bindings already resolve the associated contract
  port and that explicit contract selectors are only needed when bootstrapping
  or overriding the binding’s revision target.
- Made the Spark runtime optional by moving `pyspark` into the `spark` extra so
  runtimes that already ship PySpark are not forced to reinstall it when
  installing the integration helpers.
- `read_with_governance` once again computes fresh validations for every call
  instead of reusing cached statuses, ensuring governed reads always obtain the
  latest metrics from the service even when earlier snapshots exist.
- Streaming reads now propagate dataset identifiers into validation payloads
  when metrics originate from the data quality service so governance
  integrations receive consistent dataset metadata regardless of which backend
  generated the validation.
- Split the OpenLineage and OpenTelemetry dependencies into dedicated
  `lineage` and `telemetry` extras (with documentation updates) so installs
  only pull in those SDKs when the corresponding governance integrations are
  enabled, while CI and release workflows request the extras explicitly.
- Aligned the test extra to require `databricks-dlt` `<0.3` so the demo and
  integration suites install a compatible PySpark stack during CI runs.
- `generate_contract_dataset` now returns only an in-memory DataFrame so tests
  can persist via the regular governance write helpers when needed, and it
  inspects contract schemas directly instead of calling backend helpers.
- Spark integrations now require the `dc43-core` package so contract drafting
  and ODCS utilities rely on a single shared implementation.
- Raised the minimum `dc43-core` dependency to 0.27.0.0 so pre-release
  rewrites cover the shared helpers alongside the other internal packages.
- Bumped the package baseline to ``0.27.0.0`` so Test PyPI validation can
  continue after the ``0.26.0.0`` build was removed upstream.
- Deprecated contract- and data-product-centric helpers (`read_with_contract`,
  `write_with_contract`, their streaming counterparts, and related aliases).
  They continue to forward into the governance flow but now emit
  ``DeprecationWarning`` notices so callers migrate to the governance-only
  entry points.
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
- Expanded Spark integration tests to exercise governance-first read/write
  flows across data product bindings, DQ violations, and format guardrails, and
  aligned the helper behaviour so governance-only calls report review-required
  registrations just like the legacy contract wrappers.
- `read_with_governance` now forwards the active status strategy and enforce
  flags to the governance service so opting into draft products (for example via
  `DefaultReadStatusStrategy(allowed_data_product_statuses=("active", "draft"))`)
  behaves consistently with the contract-only helpers.
- Removed the redundant `physical_location` output binding requirement from the
  Databricks Delta demos because the Spark write request already supplies the
  Unity Catalog table path.
- `read_with_governance`/`write_with_governance` now annotate validation
  results with an observation scope (governed read slice, pre-write dataframe,
  streaming micro-batch, …) so downstream tooling can distinguish slice-level
  evaluations from full dataset verdicts.
- `VersionedWriteSpec` now treats the dataset version as optional and the
  Databricks Delta batch/streaming notebooks expose an `auto_dataset_version`
  toggle so governed runs can rely on timestamped identifiers without manually
  incrementing semantic versions in the walkthroughs.

### Fixed
- Databricks Delta batch and streaming demos now supply the registered data
  product version when issuing governed writes so notebook runs no longer
  create draft-only output ports that block enforcement.
- Governance write telemetry spans now honour dataset identifiers and versions
  from resolved plans, keeping OpenTelemetry attributes aligned with governance
  metadata even when the Spark locator infers contract-based fallbacks.
- Governance write requests now retain locator-derived dataset identifiers when
  linking contracts, so upgrading a contract no longer drops the existing
  dataset association in local governance tests.
- `generate_contract_dataset` now uses a deterministic timestamp range instead
  of depending on the current clock so repeated calls with the same seed produce
  identical rows.

