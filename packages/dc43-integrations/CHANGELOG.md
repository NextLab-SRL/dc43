# dc43-integrations changelog

## [Unreleased]

### Added
- Added `draft_contract_from_dataframe` to capture schema/metric observations
  from Spark DataFrames and return ready-to-review ODCS draft contracts using
  the shared builders from the new `dc43-core` package.

### Changed
- `generate_contract_dataset` now returns only an in-memory DataFrame so tests
  can persist via the regular governance write helpers when needed, and it
  inspects contract schemas directly instead of calling backend helpers.
- Spark integrations now require the `dc43-core` package so contract drafting
  and ODCS utilities rely on a single shared implementation.
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

