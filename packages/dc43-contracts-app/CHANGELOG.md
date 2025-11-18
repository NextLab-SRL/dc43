# dc43-contracts-app changelog

## [Unreleased]

### Added
- The integration helper sidebar now lists governed data products alongside
  contracts, letting users add product nodes, wire ports into transformations,
  and emit Spark stubs that include product bindings.
- The integration helper sidebar scrolls independently with taller catalog
  sections, adds explicit drag handles so catalog entries drop into the canvas,
  and mirrors the **Add input/output** controls in the selection panel while
  continuing to remind stewards that proposing new ports requires publishing a
  fresh product version before code generation resumes.
- Added a rich data product editor with searchable contract and dataset
  selectors, dynamic port controls, and inline custom property management so
  stewards can publish new versions without leaving the UI.
- Added Playwright regression coverage for the editor, validating contract
  search helpers, port wiring, and successful saves through the browser.

### Changed
- Dropping a governed data product onto the integration helper canvas now
  instantiates its referenced contracts and dedicated transformations
  automatically, wiring product ports into the generated nodes so stewards see
  the full lineage without manual drag-and-drop.
- Bumped the package baseline to ``0.27.0.0`` so Test PyPI validation can
  continue after the ``0.26.0.0`` build was removed upstream.
- Removed the dataset record loader/saver configuration hooks so `load_records`
  now derives history exclusively from governance APIs, exposing pipeline
  activity and validation status without expecting manual persistence helpers.
- Simplified dataset history wiring so the UI consumes loader/saver hooks from
  the active services instead of instantiating its own record store; demo runs
  continue to register their filesystem-backed providers via the service
  bootstrap.
- Dataset and contract pages now surface governance metrics, highlighting the
  latest snapshot and recent history so stewards can audit observations without
  leaving the UI.
- Introduced interactive trend charts for numeric governance metrics so teams
  can explore timeseries directly from the dataset and contract detail pages.
- Updated the Spark pipeline stub and preview tooling to call
  `read_with_governance`/`write_with_governance`, emitting governance request
  payloads in generated snippets and surfacing deprecation messaging for the
  legacy contract-based helpers.
- Spark-related extras now pull in the integrations `spark`, `lineage`, and
  `telemetry` dependencies so contracts-app installs only opt into the
  governance runtimes they need while CI/test helpers keep the optional
  packages available.
- Removed direct filesystem access from the contracts UI; dataset previews and
  history now surface only when the demo pipelines populate them, while remote
  deployments continue to focus on contract and data product metadata served by
  the configured backends.
- Dropped the ``server.store`` export from the contracts app; downstream demos
  should import the shared ``dc43_contracts_app.services.store`` adapter or call
  ``contract_service`` directly.
- Dataset catalogs and version detail pages now request dataset-scoped pipeline
  activity with inline validation statuses so the UI issues far fewer
  governance API calls when rendering individual datasets.
- Dataset catalog, versions, and detail pages now display the observation scope
  recorded alongside each governance status (pre-write dataframe snapshot,
  governed read, streaming batch, …) so analysts can tell whether a metric comes
  from a slice or the full dataset version.
- Dataset version pages now filter governance calls to the selected dataset
  version, preventing a single view from walking the entire history just to
  render one record.
- Dataset overview pages now surface dataset-wide metric trend charts and a
  sortable history table (sorted by dataset version, then contract and version)
  while the per-version detail view focuses on the selected run’s snapshot.
- Dataset trend charts now offer contract + contract-version selectors and
  treat textual numeric metrics as plottable values so historical runs from
  stores that omit dedicated numeric fields still render on the timeline.
- The dataset metrics panel now decodes JSON-wrapped values, keeps chart data in
  sync when governance stores emit numeric strings, and falls back to dataset
  versions when contract revisions are missing so the selector remains usable on
  every dataset.
- Dataset metric cards now reuse dataset history metadata when metric rows lack
  contract revisions and surface a clear “No numeric metrics” message whenever
  the backend only returns textual observations, so the selector and chart never
  render as an empty panel.
- Dataset overview trend cards now always render with a visible loading/empty
  state and keep the chart script attached even when no numeric metrics exist,
  ensuring contract selectors stay usable and it is obvious that Chart.js
  initialised instead of leaving a blank div on the dataset page.
- The dataset overview page now bundles Chart.js locally, waits for the library
  to load, and warns operators when the script cannot be fetched so the metrics
  trend card no longer stays blank when external CDNs are blocked.
- Removed the legacy `workspace` module from the contracts UI; filesystem
  helpers now live exclusively in the demo app while the standalone UI derives
  optional path hints from configuration and persists setup state under
  `~/.dc43-contracts-app` (overridable via `DC43_CONTRACTS_APP_STATE_DIR`).
- Workspace directory hints are now supplied by demo integrations via the new
  hint registration hook, so standalone deployments no longer synthesise
  filesystem defaults for setup wizard modules.
- Updated docs chat configuration and the `dc43-docs-chat-index` CLI to accept
  an optional base directory, defaulting cached embeddings to
  `~/.dc43/docs_chat/index` when no explicit `index_path` is supplied.

### Fixed
- Integration helper transformation panels now list linked data product ports
  alongside contracts so you can focus nodes and remove bindings for product
  connectors directly from the summary view.
- Data product nodes on the integration helper canvas now drag the same way as
  contracts, so you can reposition governed products without reloading the
  page.
- Editing data products that were still marked as drafts now bumps the next
  semantic version automatically, eliminating the 500 error triggered when the
  editor encountered ``*-draft`` identifiers.
- Dataset history pages now rely exclusively on the governance status matrix
  endpoint so the UI avoids spamming per-version validation lookups and skips
  pointless calls for "latest" aliases when rendering activity tables.
- Dataset history views now deduplicate repeated pipeline activity rows so each
  dataset/contract version shows up once even if the backing governance store
  returns redundant entries for the same write.
- Dataset listings now attach contracts to the dataset identifiers that
  governance runs recorded, preventing phantom rows that only contain contract
  IDs when server metadata omits explicit dataset references.
- Dataset catalog and dataset views now tolerate missing observation-scope
  metadata so historical runs recorded before the new annotations still load and
  simply display a neutral badge when the governance store lacks scope fields.
- Dataset detail pages now retry governance metric lookups without contract
  filters so backends that only persist dataset-level measurements still render
  metric tables and history charts for every run.
- Adjusted the documentation assistant to discover repository Markdown when running from
  editable installs so the chat surface no longer reports missing documentation directories.
- Treat secrets pasted into `docs_chat.api_key_env` as inline API keys automatically so misconfigured
  installs do not block the documentation assistant with missing-key warnings.
- Added the `chardet` dependency to the docs-chat optional extras so documentation indexing
  works out of the box without requiring manual module installs.
- Batched documentation embeddings during vector index creation so large Markdown and code
  trees no longer trigger OpenAI "max tokens per request" errors when the assistant starts.
- Reordered the docs assistant chat output so the processing log appears before the
  answer, keeping the final response visible as the most recent chat bubble.
- Added regression coverage that posts each module/option combination to the setup wizard
  and asserts the persisted configuration retains every provided field, guarding against
  future persistence regressions.
- Verified that the exported contracts-app and backend TOML files round-trip through the
  loader dataclasses so no wizard field goes missing in the generated configuration.
- Added a fallback serializer so contracts-app configuration dumps still produce TOML (and
  the package tests run) when `tomlkit` is absent from the environment.
