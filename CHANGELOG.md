# dc43 changelog

## [Unreleased]

### Added
- Integration helper pipeline now surfaces governed data products alongside
  contracts so you can add product nodes, wire their ports into transformations,
  and generate stubs with the correct product bindings.
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

### Changed
- The integration helper now auto-adds referenced contracts and transformation
  scaffolding when you drop a governed data product that already exposes ports,
  so the canvas immediately reflects the product’s input/output lineage.
- The `generate_contract_dataset` testing helper now returns only an in-memory
  DataFrame, leaving persistence to the governance write wrappers.
- Spark integrations now depend on the shared `dc43-core` package instead of
  embedding fallback builders.
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
- Fixed the Test PyPI publish workflow so labeled pull requests query the
  current labels before deciding whether to run, ensuring tagged branches
  actually build and upload packages for validation.
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
- Governance lineage helpers now lazy-load the OpenLineage dependency and
  gracefully skip lineage-specific tests when the package is not available,
  keeping default Spark and demo flows runnable in lean environments.
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
- Governance status lookups now tolerate legacy SQL activity tables that lack
  timestamp columns, preventing 500 errors and eliminating the fallback storm
  of per-version requests from the contracts UI.
- Updated the Delta-backed governance stores to compare version strings using
  suffix-aware keys so rc/dev builds resolve without Spark casting errors when
  fetching the latest contract or data product entries.
- Hardened the Delta-backed stores to ignore empty version markers and treat
  ``draft`` suffixes as ordered pre-releases so historical placeholder rows no
  longer prevent ``latest`` resolution.
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
