# dc43-contracts-app changelog

## [Unreleased]

### Added
- Added a rich data product editor with searchable contract and dataset
  selectors, dynamic port controls, and inline custom property management so
  stewards can publish new versions without leaving the UI.
- Added Playwright regression coverage for the editor, validating contract
  search helpers, port wiring, and successful saves through the browser.

### Changed
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
- Removed direct filesystem access from the contracts UI; dataset previews and
  history now surface only when the demo pipelines populate them, while remote
  deployments continue to focus on contract and data product metadata served by
  the configured backends.
- Dropped the ``server.store`` export from the contracts app; downstream demos
  should import the shared ``dc43_contracts_app.services.store`` adapter or call
  ``contract_service`` directly.
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
- Editing data products that were still marked as drafts now bumps the next
  semantic version automatically, eliminating the 500 error triggered when the
  editor encountered ``*-draft`` identifiers.
- Dataset history pages now rely exclusively on the governance status matrix
  endpoint so the UI avoids spamming per-version validation lookups and skips
  pointless calls for "latest" aliases when rendering activity tables.
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
