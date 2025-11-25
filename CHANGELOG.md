# dc43 changelog

## [Unreleased]

### Added
- Integration helper pipeline now surfaces governed data products alongside
  contracts so you can add product nodes, wire their ports into transformations,
  and generate stubs with the correct product bindings.
- Governance services now expose a `/governance/dataset-records` endpoint so
  portals and automation can fetch deduplicated dataset/contract/product run
  history without replaying pipeline activity on the client.
- The integration helper sidebar now scrolls independently with taller catalog
  sections, adds explicit drag handles so contracts or data products drop into
  the canvas reliably, and mirrors the product **Add input/output** controls in
  the selection panel while flagging that a fresh product version is required
  before code generation resumes.
- Added a data product editor to the contracts application with searchable
  contract and dataset selectors so definitions no longer require manual ID
  lookups.
- Added Playwright coverage for the data product editor to exercise contract
  lookup lists, port configuration, and save flows end to end.
- Added a governance status matrix endpoint that returns batched contract /
  dataset validation results to avoid repeated single-status lookups from the
  UI.
- Introduced the Spark `draft_contract_from_dataframe` helper to generate ODCS
  draft contracts (plus schema/metric observations) directly from DataFrames
  using the shared builders from the new `dc43-core` package.
- Extracted the ODCS/ODPS helpers and SemVer utilities into the standalone
  `dc43-core` distribution so services, clients, and integrations share the
  same implementation without private shims.
- Added a Databricks Delta versioning notebook to `packages/dc43-integrations`
  that automates contract evolution, governed writes, and compatibility matrix
  reporting for quick validation in notebooks or jobs.
- Added a Databricks Delta streaming notebook to `packages/dc43-integrations`
  that runs successive Structured Streaming writes under evolving contracts and
  prints the resulting governance compatibility matrix for inspection.
- Added Delta Live Tables batch and streaming notebooks so Databricks pipelines
  can reproduce the governed versioning scenario without converting the Python
  scripts manually.
- Added `log_sql` toggles to the service backend stores so Delta and SQL
  implementations can emit the statements they execute when debugging slow
  dataset or contract views.
- Unity Catalog tagging can now emit Unity tags alongside table properties.
  Enable `unity_catalog.tags_enabled` (optionally pointing `tags_sql_dsn` at a
  dedicated warehouse) to have the governance backend run `ALTER TABLE … SET/UNSET
  TAGS` automatically whenever datasets are linked to contracts.
- Added `scripts/generate_governance_demo_data.py` to seed multiple contracts,
  datasets, product bindings, and varied governance runs against the API for
  demos and screenshots.
- Governance demo seeding now accepts configurable product, contract, and run
  counts so UI screenshots can be densely populated without manual editing.
- Governance demo generation now uses Faker to craft fresh product names,
  contract identifiers, and recent run timelines so seeded UIs feel fuller and
  closer to real-world datasets.
- Governance demo seeding no longer depends on fixed base scenarios; contract
  fields, run histories, datasets, and bindings are generated end to end for a
  richer and more generic demo surface.
- Governance demo seeding now materialises matching data products and ports
  before registering runs so governance write bindings resolve cleanly against
  fresh stores.

### Changed
- `dc43-integrations` now treats Spark as an optional dependency, so
  environments that already provide PySpark can install the integrations
  without pulling in a duplicate runtime; opt into the `spark` extra to
  provision PySpark alongside the helpers when needed.
- The contracts UI now requests dataset-scoped pipeline activity with inline
  validation statuses, reducing the number of governance API calls required to
  render dataset listings and version detail pages.
- Spark governance reads now always recompute validations through the
  governance service instead of reusing cached statuses so every run reflects
  the freshest recorded metrics, even when historical snapshots exist.
- The governance demo data generator now builds multi-input/output product
  pipelines with varied run histories and consistent metric series so demo UIs
  can showcase branching dependencies, denser timelines, and datasets linked to
  multiple contract versions without manual tweaking.
- Unity Catalog hooks now treat the contract, data product, and governance tables declared in the backend configuration as reserved so only actual datasets receive `dc43.*` properties and tags even if a misconfigured client forwards those control-table identifiers.
- Unity Catalog tagging now resolves tables from each contract's `servers` block and only falls back to dataset identifiers when no Unity metadata exists. Hook builders receive the active contract backend via `LinkHookContext`, enabling the Unity integration to load contracts safely while still skipping reserved control tables and continuing when catalog updates fail due to permissions.
- Delta governance stores can now specify a `dsn`; when present, the backend reuses the SQL implementation and issues all persistence statements through the referenced Databricks SQL warehouse so remote FastAPI deployments no longer require an embedded Spark session just to update Unity-backed Delta tables.
- Streaming reads now copy dataset identifiers into validation payloads even
  when the data quality service provided the metrics so governance clients see
  consistent metadata when correlating validation results with history.
- OpenLineage and OpenTelemetry runtimes now live behind the new
  `lineage`/`telemetry` extras on the integrations and service-client
  packages so deployments opt into those SDKs only when emitting lineage or
  telemetry events.
- CI, release automation, and the local `scripts/test_all.sh` helper now
  install the integrations/service-client extras for Spark, OpenLineage, and
  OpenTelemetry explicitly so governance suites keep the optional runtimes
  available during automated tests.
- Updated the integrations test extra to stay on `databricks-dlt` `<0.3` so CI
  installs the same PySpark toolchain as the demo application.
- The integration helper now auto-adds referenced contracts and transformation
  scaffolding when you drop a governed data product that already exposes ports,
  so the canvas immediately reflects the product’s input/output lineage.
- The `generate_contract_dataset` testing helper now returns only an in-memory
  DataFrame, leaving persistence to the governance write wrappers.
- Spark integrations now depend on the shared `dc43-core` package instead of
  embedding fallback builders.
- Databricks Delta batch and streaming demos now ship an `auto_dataset_version`
  toggle and updated helpers so dataset identifiers fall back to the timestamped
  versions that production pipelines use instead of forcing manual increments in
  the walkthrough notebooks.
- Unity Catalog governance hooks now rely exclusively on Databricks SQL DSNs,
  sanitise Unity tag names, skip reserved properties such as `owner`, and warn
  instead of aborting governance updates when the Databricks token lacks
  permission to alter a target table.
- Service backend configuration now honours explicit `governance_store.metrics_table`
  entries (and the `DC43_GOVERNANCE_METRICS_TABLE` override) so governance SQL
  stores stop deriving table names when the operator already provided one.
- Unity Catalog tagging now uses a Databricks SQLAlchemy DSN (`unity_catalog.sql_dsn`)
  to run `ALTER TABLE … SET/UNSET TBLPROPERTIES` instead of the unsupported
  workspace `tables.update` call, and the Databricks guide documents the new
  configuration plus cleanup commands.
- Governance stores now expose batch status lookups so matrix endpoints and
  pipeline activity enrichment reuse a single SQL query instead of issuing one
  lookup per dataset/contract combination.
- The contracts UI now bundles Chart.js locally, waits for the script to load,
  and surfaces inline warnings when the library cannot be fetched so dataset
  metric charts no longer render as blank cards when CDN traffic is blocked.
- Dataset overview pages now render dataset-wide metric charts and sortable
  history tables while individual dataset version views limit governance calls
  to the selected run, reducing redundant backend work when inspecting a single
  version.
- Dataset overview charts now expose contract and contract-version selectors and
  treat numeric strings as plottable values, so governance stores that only
  persist textual metric payloads still populate the trend chart for the
  selected filters. The selector now falls back to dataset versions when
  contract revisions are missing, and service backends preserve textual metric
  payloads while still populating ``metric_numeric_value`` so chart data remains
  consistent across SQL, Delta, and filesystem stores.
- The demo runner now starts when invoked as a module (``python -m
  dc43_demo_app.runner``) and logs the backend, contracts app, and UI endpoints
  it booted so local quickstarts keep the full stack online in one terminal.
- Dataset overview metric panels now reuse dataset history metadata to keep the
  contract-version dropdown enabled even when metric rows omit revision fields
  and surface a clear “No numeric metrics” message whenever the backend only
  returns textual observations, preventing the chart from rendering as an empty
  card.
- The datasets catalog drops the sparsely populated “Data products” column and
  instead renders governed product associations on the dataset detail page,
  where each card lists the bound product, port, contract, and latest dataset
  version recorded for that lineage.
- Dataset trend cards now always render with a visible loading/empty message and
  keep their chart script attached even when no numeric metrics are present, so
  operators can confirm the selector state and Chart.js initialisation instead
  of staring at a blank block when governance stores only emit textual values.
- Updated internal dependency floors to align the new `dc43-core` package with
  the 0.27.0.0 release train so Test PyPI rewrites pick up the shared helper
  requirement during pre-release validation.
- Bumped the package baseline to ``0.27.0.0`` so Test PyPI validation can
  continue after the ``0.26.0.0`` build was removed upstream.
- Added a `publish-test-pypi` pull request label that triggers CI to build
  release artifacts and upload them to Test PyPI for pre-release validation,
  mirroring the release workflow when any package code changes even if the
  version has not been bumped yet.
- Test PyPI publishes now append an `rc` suffix based on the GitHub run number
  so repeat validation uploads do not require manual version bumps or
  requirement updates.
- The Test PyPI helper now rewrites internal dependency floors to accept the
  active pre-release stage so rc builds install cleanly without waiting for the
  final release tags.
- Documented the Test PyPI helper's reliance on PEP 440 ordering and added
  coverage that proves generated `dev`/`rc` artefacts remain sortable by
  `pip`.
- Spark governance helpers now tag validation results with an observation scope
  (pre-write dataframe, streaming micro-batch, governed read, …) and the
  contracts UI surfaces that scope beside each dataset record so you can
  distinguish slice-level snapshots from full dataset verdicts when comparing
  metrics.
- Contracts app dataset version pages now fetch only the selected run and reuse
  the backend’s batched status queries, preventing a single view from rebuilding
  the entire compatibility matrix.
- Fixed the Test PyPI publish workflow so labeled pull requests query the
  current labels before deciding whether to run, ensuring tagged branches
  actually build and upload packages for validation.
- The Databricks Delta demos and guide dropped the redundant
  `physical_location` output binding entry because the table location already
  comes from the Spark write request.
- Databricks Delta versioning demos now pin governed writes to the released
  data product version so notebook runs no longer pause on draft output ports.
- Governance configuration docs now explain how SQL and Delta stores derive
  `metrics_table` defaults from `_dq_status` identifiers so operators know when
  to override the value explicitly.
- Removed the demo/contract helpers that manually persisted dataset records so
  the UI and pipelines rely solely on governance service APIs for run history,
  adding fixtures and helpers to tests to generate sample data on demand.
- Removed the contracts app dataset record store wiring so the UI simply uses
  service-provided loaders, keeping history ownership with whichever runtime
  configures the service clients (the demo still registers its filesystem
  helpers).
- Surfaced governance metrics in the contracts app dataset and contract views
  so operators can inspect recorded observations directly in the UI.
- Added interactive metric trend charts to those pages so numeric observations
  plot as timelines with tooltips highlighting dataset versions and contract
  revisions.
- Deprecated contract/data-product Spark IO shims (`read_with_contract`,
  `write_with_contract`, and related helpers) in favour of the
  governance-first wrappers. Compatibility calls now emit
  ``DeprecationWarning`` messages to steer pipelines towards
  `read_with_governance`/`write_with_governance`.
- Governance Spark IO now enforces data product status policies and accepts
  explicit version constraints for input/output bindings so pipelines can
  block on draft products by default or target specific releases when
  required.
- Centralised the OpenLineage runtime dependency with the service clients so
  demo and developer installs inherit the lineage runtime transitively instead
  of duplicating requirements across packages.
- Updated the CI workflow to install OpenLineage explicitly for jobs that skip
  dependency resolution so demo and governance suites no longer fail when
  optional extras are omitted.
- Governance backends now honour data product version selectors and source
  contract requirements when resolving read/write contexts, failing fast on
  draft or mismatched products before registration occurs.
- Governance read/write requests now forward status allowances (including
  `DefaultReadStatusStrategy` overrides and enforcement toggles) to the
  governance service so draft products can be opted into intentionally while
  the backend continues to block unexpected states by default.
- The continuous integration workflow now triggers only for pull request
  events (plus manual dispatch) so pushes to shared branches no longer spawn
  duplicate runs alongside the PR build.

### Fixed
- Governance demo seeding now publishes generated contracts to the contracts
  service before registering governance runs, preventing write context
  resolution failures when the contract store is empty.
- Filesystem governance stores now expose the pipeline activity directory
  helper used by dataset listings so the `/governance/dataset-records`
  endpoint no longer raises attribute errors.
- Governance write telemetry spans now prefer dataset identifiers and versions
  from resolved governance plans, keeping OpenTelemetry attributes aligned with
  pipeline metadata and avoiding contract-id fallbacks in tests.
- Governance stores now extract metric observations from validation payload
  details when the ``metrics`` attribute is empty, ensuring `dq_metrics`
  tables (and the contracts UI) show dataset measurements recorded by remote
  backends.
- Delta governance stores now derive `*_dq_status` metric tables as
  `*_dq_metrics` when `metrics_table` is omitted so Databricks demo configs
  continue writing into the expected governance schema without extra
  configuration.
- Governance write helpers once again preserve dataset links derived from
  dataset locators, so upgrading a contract keeps previously registered dataset
  references intact while telemetry continues to use governance plan metadata.
- Declared ``attrs`` as a core dependency so OpenLineage governance helpers
  import cleanly without manual dependency installs.
- Integration helper transformation details now surface linked data product
  ports, so the focus/remove actions work for governed product bindings as well
  as contract connectors.
- Data product nodes dropped onto the integration helper canvas now drag just
  like contract cards, so you can rearrange layouts without refreshing the
  page.
- The contracts app data product editor now bumps draft-suffixed versions
  without crashing, so editing ``*-draft`` releases no longer triggers 500
  errors when calculating the suggested version.
- Contracts app status history now honours the governance status matrix
  endpoint, trimming redundant per-pair status requests and avoiding failures
  when remote backends return pre-encoded validation payloads.
- Contracts app dataset history pages now deduplicate repeated pipeline
  activity entries so each dataset/contract version appears only once even when
  the backing governance store emits redundant rows.
- Dataset overview pages now always include the metric trend script whenever the
  chart card is rendered, ensuring Chart.js initialises and replaces the loading
  message even when templates reuse the summary partial multiple times.
- Contracts app dataset listings now attach contracts to the dataset IDs that
  governance runs recorded, preventing phantom dataset rows that only differ by
  contract ID when server metadata omits explicit dataset identifiers.
- Contracts app dataset listings now tolerate records captured before
  observation-scope metadata shipped, falling back to a neutral badge so the
  catalog keeps loading even when governance stores have not populated the new
  fields.
- Contracts app data product catalog now infers run history by cross-referencing
  dataset identifiers and contract bindings with product output ports, so
  product pages show recorded runs even when older pipeline events lacked
  explicit product metadata.
- Contracts app dataset detail pages now retry governance metric lookups
  without contract filters so backends that only persist dataset-level
  measurements still surface snapshots and charts for every recorded version.
- Governance pipeline activity endpoint now encodes inline validation results
  before responding, so include-status requests no longer raise FastAPI
  serialization errors when backends return `ValidationResult` instances.
- Governance status lookups now tolerate legacy SQL activity tables that lack
  timestamp columns, preventing 500 errors and eliminating the fallback storm
  of per-version requests from the contracts UI.
- Updated the Delta-backed governance stores to compare version strings using
  suffix-aware keys so rc/dev builds resolve without Spark casting errors when
  fetching the latest contract or data product entries.
- Hardened the Delta-backed stores to ignore empty version markers and treat
  ``draft`` suffixes as ordered pre-releases so historical placeholder rows no
  longer prevent ``latest`` resolution.
- Delta governance stores now purge previous status rows before appending the
  latest result so Databricks tables (and the contracts UI) no longer show the
  same dataset version multiple times in the compatibility matrix.
- Introduced governance-first Spark IO wrappers and updated documentation/tests
  so pipelines can rely on a single governance client instead of wiring
  contract/data-quality services manually.
- Governance registration now reloads explicitly requested data product
  versions before enforcing bindings so read/write activity honours historical
  releases instead of comparing against the latest draft returned by the
  registration helpers.
- `read_with_governance` and its streaming counterpart now accept
  `GovernanceReadContext` payloads so pipelines can declare contract references
  or data product bindings explicitly when resolving datasets through
  governance.
- `read_with_governance`/`write_with_governance` (and streaming variants) now
  take `GovernanceSparkReadRequest`/`GovernanceSparkWriteRequest` containers,
  reducing function signatures to a single governance client plus an orchestration
  payload that describes contracts, data product bindings, and Spark overrides.
- Governance Spark helpers now position the request payload immediately after
  the Spark/DataFrame argument, with the governance client supplied next, so
  call sites read in the same order as the underlying orchestration flow.
- Demo Spark and streaming scenarios now rely on the governance helpers,
  building read/write requests from scenario context so presenters initialise
  only the governance client while still recording contract metadata, dataset
  versions, and status payloads in the workspace registry.
- The contracts app no longer reads datasets or data product snapshots from the
  local workspace; dataset history and previews are provided by the demo-only
  pipelines while remote deployments focus on contract and data product
  metadata surfaced by the service backends.
- Delta Live Tables helpers now resolve contracts and expectation plans through
  the governance client, so notebooks bind contracts using the same
  governance-first contexts as the Spark IO wrappers.
- Renamed the DLT decorators to ``governed_expectations``/``governed_table``/
  ``governed_view`` so annotation names align with the governance-only
  orchestration model.
- Clarified in the DLT docs and annotations that they rely exclusively on the
  governance service, mirroring the `read_with_governance`/`write_with_governance`
  entry points.
- Updated the Spark setup bundle and integration helper stubs to emit
  governance-only read/write snippets and refreshed the getting-started guides
  (local, remote, Databricks, contracts app) to showcase
  `read_with_governance`/`write_with_governance` plus the new request objects.
- Retired the ``server.store`` alias from the contracts app; import
  ``dc43_contracts_app.services.store`` or use ``contract_service`` when the
  demo pipelines need direct store access.
- Guarded the service-clients bootstrap tests with `pytest.importorskip` so the
  package test suite skips gracefully when optional backend dependencies are not
  installed.
- Made `dc43_service_clients` lazy-load its bootstrap helpers so importing the
  package no longer requires `dc43_service_backends`, ensuring the bundled
  `pytest` entry point works in isolated client-only environments.
- Deferred importing the SQL governance store until SQLAlchemy is available so
  client-only environments can run the service-client test suite without pulling
  in optional backend dependencies.
- Extended Spark governance coverage so read/write helpers mirror the
  contract-based behaviours when data product registrations require review,
  added comprehensive governance-first parity tests for data product bindings,
  DQ blocks, and format warnings, and taught the governance backend to surface
  review-required registrations instead of silently continuing.
- The contracts app no longer initialises or writes to filesystem workspaces;
  docs chat caches now default to `~/.dc43/docs_chat/index` and operators can
  relocate setup wizard persistence via `DC43_CONTRACTS_APP_STATE_DIR` while
  the demo application retains the filesystem helpers for tutorials.
- Removed workspace directory hints from the contracts app; demo integrations
  now register optional filesystem paths when running the bundled scenarios,
  keeping the standalone UI focused on service-backed metadata.

  well-supported serializer and match the loaders' expectations across the setup wizard,
  contracts UI, and backend services.
- Hardened the configuration docs by adding regression tests that assert every wizard field
  and dataclass option is documented across the reference guides.
- Expanded the service backend configuration guide with Unity Catalog workspace examples so
  Delta deployments spell out how to capture hosts, tokens, and CLI profiles in TOML.

### Fixed
- Added a concurrency guard to the CI workflow so only the latest pull request
  run continues, preventing duplicate Test PyPI rc publishes when multiple
  events fire in quick succession.
- Updated the `dc43-demo` launcher to merge any exported `DC43_CONTRACTS_APP_CONFIG`
  file into the generated workspace configuration so docs-chat overrides stay
  active instead of being reset to the default template.
- Prevented the docs assistant from logging credential sources and taught it to
  locate repository Markdown when running from editable installs so local demos
  no longer report missing documentation directories.
- Coerce docs-chat secrets that are accidentally pasted into `docs_chat.api_key_env`
  into the dedicated `api_key` field so the assistant starts without confusing
  missing-key warnings.
- Added the `chardet` dependency to the docs-chat optional install so LangChain's
  Markdown loader runs without missing-module errors during documentation indexing.
- Batch docs assistant embedding requests so large repositories stay under OpenAI's
  per-request token limits instead of failing with 400 errors during index builds.
- Ensure the docs assistant displays its final response as the last chat bubble,
  moving the processing log above the answer so users no longer mistake the
  status summary for the reply.
- Added regression coverage that posts each setup wizard configuration option and
  asserts the saved `setup_state.json` retains every provided field so future
  changes cannot silently drop user input.
- Added a lightweight TOML writer fallback so backend and contracts configuration
  dumps continue to work (and their tests run) even when `tomlkit` isn't installed
  in the execution environment.
