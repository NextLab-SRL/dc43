# dc43-integrations changelog

## [Unreleased]

### Added
- Added metric computation support in Spark for ODCS v3.1.0 `duplicateValues` (single & multi-column compound keys), `missingValues` (with custom missing lists), `invalidValues` (enum and regex patterns), and `rowCount`.
- Introduced `ContractDDLBuilder` to generate and execute strict `CREATE TABLE IF NOT EXISTS` DDL statements matching the Data Contract schema, supporting `NOT NULL` constraints (`required: true`), `PRIMARY KEY` (`primaryKey: true`), `PARTITIONED BY` (`partitioned: true`), `CLUSTER BY` (Liquid Clustering), and `TBLPROPERTIES`.
- Added `ddl_modifier` and `table_properties` in `GovernanceSparkWriteRequest` to allow custom DDL enrichment during table creation.
- Added error capturing and observability for `post_write` governance interceptors (e.g. Unity Catalog tagging), recording failures into `ValidationResult.errors` and respecting the `enforce` flag.

### Fixed
- Fixed a `NameError: name 'build_spark_sql_ref' is not defined` inside `BaseDeclareExecutor._evaluate_inputs` when executing `declare_with_governance`.
- Exported `GovernanceSparkDeclareRequest`, `GovernanceDeclareContext`, `build_spark_sql_ref`, and `BaseDeclareExecutor` in `dc43_integrations.spark.io`.


## [0.42.0.0] - 2026-05-21

### Added
- Introduced `MutableStructType` subclass of Spark's `StructType` to support fluent, non-destructive schema manipulation (`drop`, `keep_only`, `rename`, `update_type`, `add_field`).
- Updated `dataframe_schema_from_contract` to return a `MutableStructType` for easy modification before use with downstream Spark functions like `from_csv`.
- Enhanced Spark executors (read, write, revalidator) and data quality metric computations to catch PySpark SQL / execution exceptions (such as `CANNOT_PARSE_TIMESTAMP` or check filter errors) and report them as validation errors (`ok=False`) instead of raising unhandled exceptions and crashing user scripts. This ensures the configured violation strategies (e.g., quarantine) can execute successfully on invalid format inputs.
- Added `declare_with_governance` to the Spark IO module to support declarative permanent view deployment. This interprets a templated SQL query, dynamically discovers and evaluates all inputs (applying data quality rules and time travel), and creates a Databricks catalog view safely.
- Added `build_spark_sql_ref` in `dc43_integrations.spark.io.common` to translate `DatasetResolution` objects securely into Spark SQL text representations, interpreting time travel configurations like `VERSION AS OF`.

### Changed
- Prioritized `physicalType` over `logicalType` in `dataframe_schema_from_contract` when converting properties to Spark data types.
- Aligned documentation, examples, and Spark integration helper docstrings to promote contract-first (`from_contract`) and port-first (`from_port`) patterns as the primary interfaces, removing legacy and invalid usages of low-level `dataset_id` references.

### Fixed
- Fixed an `AttributeError` in `write_with_governance`, `read_with_governance`, `declare_with_governance`, and `merge_with_governance` when requests or contexts are passed as raw mappings/dictionaries instead of fully instantiated dataclass objects. The parsed context object is now correctly assigned back to the request, and `pipeline_context` is resolved safely.
- Improved `ValueError` when contract status check fails, providing a clear suggestion for developers on how to customize `allowed_contract_statuses` to accept other statuses (like `candidate`) during development or validation phases.
- Fixed an `AttributeError` in `write_with_governance` where data product status attributes were accessed directly on violation strategies without checking for their presence. This allows wrapper strategies like `StrictWriteViolationStrategy` to be used without errors.

## [0.41.0.0] - 2026-03-31

### Added
- Introduced the `GovernanceInterceptor` protocol in the Spark IO integration to unify both `pre_write` data mutations and `post_write` infrastructure side-effects (e.g., Unity Catalog tagging). 
- Added dynamic configuration for interceptors via the `DC43_GOVERNANCE_INTERCEPTORS` environment variable or Spark configuration arrays.

### Removed
- Removed `ContractBasedTransformer` and its associated application logic to eliminate technical debt, intentionally favoring the new full-lifecycle Interceptor pattern over backward compatibility.

## [0.40.0.0] - 2026-03-19

### Added
- Added `merge_with_governance` to the Spark IO module to support Delta Lake Upserts while enforcing data quality, contract resolution, and reporting telemetry exactly like `write_with_governance`.

## [0.39.0.0] - 2026-03-18

### Changed
- Version aligned to 0.39.0.0

## [0.38.5.0] - 2026-03-18

### Fixed
- Fixed a Spark pickling bug in `StreamingObservationWriter` where the governance client failed to reconstruct on worker nodes. This resolves an issue where streaming micro-batches would silently bypass governance evaluation and omit metric publication.


## [0.35.0.0] - 2026-03-09

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

