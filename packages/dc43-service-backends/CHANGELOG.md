# dc43-service-backends changelog

## [Unreleased]

### Added
- Added support for checking float format using `logicalTypeOptions` properties `decimalSeparator` and `thousandsSeparator` for string fields.
- Added support for checking integer format using `logicalTypeOptions` property `thousandsSeparator` for string fields.

### Fixed
- Fixed a bug where `exact_format` SQL predicate generation used `to_timestamp` which crashed under Spark's ANSI mode when encountering invalid values. Changed to `try_to_timestamp` to return `NULL` on parsing failure instead of raising exceptions.


## [0.42.0.0] - 2026-05-21

### Added
- Added Basic Authentication support (username and password credentials) for Collibra HTTP Contract Store adapter and configuration loaders.
- Added corresponding environment variable overrides (`DC43_CONTRACT_STORE_USERNAME` and `DC43_CONTRACT_STORE_PASSWORD`).
- Added an integration verification script `test_collibra_basic_auth.py` for testing credentials against the Collibra HTTP adapter and backend.
- Added a robust `MockCollibraService` supporting dynamic assets, relations, Search API, and Data Product API v1 for comprehensive offline testing of Collibra integrations.
- Added `is_null` rule to Data Quality expectation specifications and predicate mappings.
- Added support for ODCS `logicalTypeOptions.format` date format validation via the `exact_format` rule and Spark `to_timestamp` predicates.

### Changed
- Migrated `HttpCollibraContractAdapter` to target the official Collibra Data Product API v1 (`/rest/dataProduct/v1`) with multipart YAML upload/download.
- Integrated a dual lookup strategy in Collibra adapter: Option A (direct technical UUID mapping) cascading to a graph-based relational traversal (Data Product ➔ Port ➔ Data Contract) when needed.
- Refactored `dc43_service_backends.data_quality.backend.engine.expectation_specs` to use a Registry Pattern for dynamically dispatching and extracting ODCS quality validations into Spark/SQL ExpectationSpecs.

### Fixed
- Fixed a bug in `LocalContractServiceBackend.link_dataset_contract` that attempted to write back the contract via `store.put()`, triggering HTTP `409 Conflict` errors on Collibra stores where the version was already registered.

### Deprecated
- Deprecated `link_dataset_contract` and `get_linked_contract_version` on the `ContractServiceBackend` interface in favor of delegating dataset-contract linkage exclusively to the `GovernanceServiceBackend`.

## [0.41.0.0] - 2026-03-26

## [0.40.0.0] - 2026-03-19

### Changed
- Relaxed the structured dependency `open-data-contract-standard` constraint from `==3.0.2` to `>=3.0.2,<4.0.0` to support ODCS 3.1.0 and future minor updates.

## [0.39.0.0] - 2026-03-18

### Changed
- Version aligned to 0.39.0.0

## [0.38.5.0] - 2026-03-18

### Fixed
- Fixed an indentation error in `SQLGovernanceStore.save_status` that triggered `ResourceClosedError`s and disrupted metric extraction during data quality evaluations.


## [0.35.0.0] - 2026-03-09

### Added
- Contract, data product, and governance stores now expose `log_sql` toggles
  plus matching `DC43_*_LOG_SQL` environment overrides so Delta and SQL
  implementations can emit the statements they execute when debugging backend
  traffic.
- Governance backends now expose a `/governance/dataset-records` endpoint that
  returns deduplicated dataset run metadata (contract, product port, latest
  status) so portals and automation no longer replay pipeline activity to build
  dataset histories.
- Unity Catalog tagging can now emit Unity Catalog tags in addition to table
  properties. Set `unity_catalog.tags_enabled = true` (and optionally
  `tags_sql_dsn`) to have the backend execute `ALTER TABLE … SET/UNSET TAGS`
  whenever datasets link to contracts, and configure `[unity_catalog.static_tags]`
  for custom ownership labels.

### Changed
- Core ODCS/ODPS helpers now live in the shared `dc43-core` package and this
  distribution imports them directly, ensuring all runtimes share the same
  implementation without maintaining duplicate copies.
- Raised the `dc43-core` dependency floor to 0.27.0.0 so rc rewrites keep the
  shared helper package in sync with the service backends during Test PyPI
  validation.
- Bumped the package baseline to ``0.27.0.0`` so Test PyPI validation can
  continue after the ``0.26.0.0`` build was removed upstream.
- Governance backends and stores now surface `list_datasets` and pipeline
  activity/status lookups so UI clients can assemble dataset history directly
  from the service without relying on demo-specific record stores.
- Pipeline activity endpoints now accept an `include_status` flag that embeds
  the persisted validation result alongside each dataset/contract combination,
  avoiding additional status matrix lookups for consumers that only need the
  latest verdict.
- Governance stores now expose `load_status_matrix_entries` so batched status
  lookups reuse a single SQL/Delta query instead of issuing one request per
  dataset/contract combination.
- Unity Catalog tagging now relies on `unity_catalog.sql_dsn` and the Databricks
  SQLAlchemy driver to issue `ALTER TABLE … SET/UNSET TBLPROPERTIES`, replacing
  the unsupported workspace `tables.update` call and documenting the new
  configuration knobs plus cleanup steps.
- The Unity Catalog linker now removes the unused workspace/Spark pathway,
  sanitises Unity tag keys (replacing reserved characters with underscores),
  drops reserved property names such as `owner`, and catches permission errors
  so failed catalog updates only emit warnings instead of interrupting
  governance flows.
- Unity Catalog hooks now treat contract, data product, and governance tables declared in the backend configuration as reserved, guaranteeing that only dataset identifiers passed to `link_dataset_contract` receive properties or tags even if a client forwards the wrong table name.
- Unity Catalog tagging now resolves target tables directly from each contract's `servers` block (falling back to dataset identifiers only when no Unity metadata exists) and the hook builders receive a `LinkHookContext` with the active contract backend so catalog updates can safely load contracts while continuing on permission failures.
- Delta governance stores can now specify a `dsn`; when present, the builder reuses the SQL implementation so Databricks SQL warehouses handle every insert/update and remote FastAPI deployments no longer need to start a Spark session purely to persist governance metadata in Delta tables.
- Local governance backends now expose contract resolution helpers and include
  underlying validation payloads when returning `QualityAssessment` objects so
  clients relying solely on the governance layer retain access to detailed data
  quality results.
- Governance storage now guards the SQL backend import so environments without
  SQLAlchemy can still import the package and run client-only tests without
  pulling in optional dependencies.
- Governance registration hooks now raise review-required errors when data
  product input/output registrations create new drafts, matching the behaviour
  of the legacy Spark contract helpers so governance-first pipelines surface the
  same guardrails.
- Governance context resolution now enforces data product version selectors and
  source contract requirements so reads and writes block on draft products or
  mismatched upstream contracts before producing new registrations.
- Governance backends now honour caller-provided data product status policies,
  allowing draft versions to resolve when the context explicitly permits them
  while continuing to block unexpected states by default during resolution and
  registration.
- The test extra now depends on `dc43-service-clients[lineage]` so lineage
  helpers continue to run during backend tests even though OpenLineage is an
  optional dependency.

### Fixed
- Filesystem governance stores once again expose the pipeline activity
  directory helper so dataset listings and `/governance/dataset-records`
  responses no longer crash with missing attributes.
- Delta governance stores now supply explicit schemas when persisting status,
  link, and activity records so Spark no longer fails to infer field types when
  optional values (such as lineage payloads) are null.
- Governance stores now pull metric observations from validation detail
  payloads when the explicit ``metrics`` attribute is empty so SQL/Delta/HTTP
  backends continue populating `dq_metrics` tables even when upstream
  validations serialise observations only inside `dq_status` rows.
- Delta governance stores now detect `*_dq_status` table names and reuse the
  shared prefix when deriving the metrics table, keeping Databricks demo
  deployments writing into `...dq_metrics` even when `metrics_table` is not set
  explicitly.
- Hardened the governance status matrix endpoint so mixed payload types (for
  example, pre-encoded validation dictionaries) no longer trigger 500 errors
  when UI clients request batched status snapshots.
- SQL governance stores now fall back gracefully when legacy activity tables
  are missing the `updated_at` column, keeping batched status lookups and
  inserts working without manual schema migrations.
- SQL governance activity lookups now include the dataset identifier and version
  from the table columns when payloads omit those fields, ensuring clients can
  enumerate available dataset versions even for legacy records.
- SQL governance status lookups now tolerate duplicate historical rows by
  selecting the most recent payload, preventing `MultipleResultsFound` errors
  when legacy tables contain redundant entries.
- SQL governance stores now honour explicit `metrics_table` values (and derive
  `_dq_metrics` from `_dq_status` identifiers just like the Delta store), so the
  contracts app and remote deployments read metrics from the populated table
  even when the configuration omits a dedicated metrics entry.
- `load_config` now preserves `governance_store.metrics_table` entries (and the
  `DC43_GOVERNANCE_METRICS_TABLE` override), ensuring the bootstrapper wires the
  configured table into SQL stores instead of falling back to the legacy
  `dq_metrics` default.
- SQL, Delta, filesystem, and in-memory governance stores now preserve textual
  metric payloads (instead of JSON-encoding them twice) while still populating
  ``metric_numeric_value`` for numeric strings, ensuring downstream UIs can plot
  dataset trends even when validations provide numbers as strings.
- Pipeline activity endpoints now encode inline `ValidationResult`s before
  returning JSON, preventing FastAPI from raising 500 errors when
  `include_status=true`.
- ODPS serialisation raises a descriptive error when non data-product objects
  (for example, Open Data Contracts) are passed to the helper, steering callers
  towards the correct client API instead of surfacing an attribute error.
- Delta-backed contract and data product stores now compare version strings with
  suffix-aware keys so rc/dev builds sort correctly without triggering Spark
  cast errors when resolving the latest entries.
- Delta-backed stores now ignore empty version markers and rank ``draft``
  suffixes as pre-releases, ensuring ``latest`` resolution succeeds even when
  historical rows contain placeholder records.
- Data product registration during read/write activity reloads explicit binding
  versions before enforcement so requested drafts or historical releases are
  validated rather than the latest product returned by the registration helper.
- Governance write registration skips automatic data product evolution when the
  binding pins an explicit version, preventing Databricks demos (and other
  pipelines that manage releases manually) from tripping review-required errors
  after they attach custom port metadata such as physical locations.
- Governance read registration now mirrors that behaviour for pinned inputs so
  consumers that attach lineage metadata or source references don't create new
  drafts when they intentionally target an existing release.
- Delta governance stores now delete existing status rows for a dataset/version
  before recording a new verdict, preventing duplicate compatibility entries in
  Databricks tables and in the contracts application history views.

