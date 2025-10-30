# dc43-contracts-app changelog

## [Unreleased]

### Changed
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

## [0.22.0.0] - 2025-10-25
### Changed
- No functional updates. Bumped the package metadata for the 0.22.0.0 release
  train.

## [0.21.0.0] - 2025-10-23
### Added
- The setup wizard now exposes a sample generator button on Step 2 that loads
  realistic placeholders from `static/setup-wizard-template.json`, letting
  contributors populate every selected module before refining the values for
  their environment.
- Setup bundle archives now ship a ready-to-run `requirements.txt`, cross-platform
  environment bootstrap scripts, and Docker build/publish helpers so teams can
  stand up virtual environments or prebuilt images without manual packaging.
- Setup bundle exports now include full Spark and Delta Live Tables example
  projects (code modules, README, and ops helpers) alongside the
  `examples/pipeline_stub.py` entrypoint so teams can start from a realistic
  integration scaffold.
- Pipeline bundle generator now loads integration-provided pipeline stub
  fragments so Spark, Delta Live Tables, and future runtimes ship their own
  helper code without modifying the contracts app.
- Documented contract status guardrails in the integration helper stub and notes so generated Spark
  snippets explain how to opt into draft or deprecated contracts safely.
- Introduced a grouped, accessibility-friendly setup wizard with step badges, reset controls, and a
  live architecture diagram that highlights the components a user selects across contracts,
  products, quality, and governance modules.
- Bundled environment exports now emit TOML configs, Terraform stubs, and launch scripts tailored to
  the selected deployment targets so new installations can jump straight into provisioning.
- Added a pipeline integration module covering Spark and Delta Live Tables runtimes so the wizard
  captures orchestration credentials and the exported helper script shows how to initialise the
  chosen engine alongside the dc43 backends.
- Added a documentation chat experience powered by LangChain and Gradio, including configuration
  defaults, a Gradio-mounted UI, and a REST endpoint for programmatic access.
- Introduced a documentation assistant module in the setup wizard so exported bundles capture the
  docs chat configuration alongside other deployment metadata.
- Extended docs chat configuration with an optional inline `api_key` field so private TOML files can
  store credentials directly when environment variables are impractical.
- Added a `dc43-docs-chat-index` CLI so operators can pre-compute the documentation assistant's
  FAISS cache using the same configuration as the FastAPI deployment.
- Updated the docs-chat index CLI to validate configuration readiness before warming up and to
  print a summary of the indexed sources when the cache build succeeds.
- Switched the default docs chat embedding provider to Hugging Face so cache builds run locally by
  default while leaving OpenAI as an opt-in for hosted embeddings; templates and docs reflect the
  new default values.
- Published a comprehensive configuration reference that lists every setup wizard module,
  exported field, and related environment override so operators can review available options in one place.

### Changed
- Setup wizard exports, backend configuration, and docs-chat helpers now agree
  on the `workspace_url` field and new regression tests cover the shared TOML
  serializers for both the contracts UI and service backend bundles.
- Setup bundle archives now include per-module TOML exports capturing the raw
  wizard field values, and regression coverage verifies every submitted value
  is written to the generated configuration files.
- The setup architecture view only renders modules that have been explicitly selected or are
  required by user-driven dependencies, preventing unrelated services from appearing in fresh
  configurations.
- Validation results storage now lives in the storage foundations step so Delta, SQL, filesystem,
  and HTTP backends are visible alongside contract and product persistence choices.
- The setup architecture overview groups the pipeline footprint versus remote hosting, surfaces the
  validation results store, and links quality runs back to their persistence target so operators can
  see how governance data flows through the deployment.
- Architecture groupings now distinguish local runtime choices from hosted deployments so the
  diagram no longer lists local Python orchestration under remote hosting and highlights the new
  pipeline integration node.
- Rebranded the UI to the "dc43 app", added a docs chat navigation entry, and documented the
  configuration knobs required to enable the assistant in deployments.
- Clarified installation guidance for the docs chat assistant so source checkouts and PyPI
  consumers know which pip command enables the optional dependencies.
- Documented that mixing the meta package demo extra with a direct
  `dc43-contracts-app[docs-chat]` install in the same environment leads to pip conflicts, and
  pointed contributors at the single-command workflow.
- Clarified that docs chat configuration requires exporting `DC43_CONTRACTS_APP_CONFIG` and that
  `docs_chat.api_key_env` holds the name of the environment variable containing the provider key.
- Updated docs, templates, and wizard guidance to highlight the new inline key support and the
  `dc43-demo --config/--env-file` launcher flags for local runs.
- Reworked the docs chat question-answer prompt so responses summarise the retrieved Markdown,
  cite file names, and explicitly call out helper APIs (for example `read_with_contract`
  and status strategies) when users ask how to plug dc43 into Spark pipelines instead of
  defaulting to "I don't know".
- Expanded the docs chat helper to index repository source code, respect configurable
  `code_paths`, and forward OpenAI reasoning effort hints so the assistant can tackle
  complex integration prompts with code-backed answers.
- Limited auto-discovered docs chat code roots to the dc43 repository so editable installs no
  longer scan unrelated mono-repo siblings and breach embedding rate limits.
- Accepted hyphenated `code-path` keys in docs chat TOML configuration and taught the
  offline index CLI to echo the resolved sources and providers before warm-up so
  misconfigurations are easier to spot during cache builds.
- Streamed docs chat progress as concise updates and now normalise Gradio
  message payloads so answers render in their own chat bubble while the full
  processing log appears in a separate collapsible message.
- Added a dc43-specific guardrail to the docs chat runtime so off-topic prompts get a polite
  reminder that the assistant is limited to project documentation and helper workflows.
- Triggered the docs chat runtime warm-up in the background during app startup, cached the
  FAISS index between runs, and now relay the queued warm-up progress to both the UI and
  application logs (including the explicit warm-up wait message returned from the REST API)
  so operators can follow each stage even when the cache build starts before the first
  question, all without blocking the server from starting.
- Added Hugging Face embedding support alongside OpenAI so large repositories can be indexed
  offline; templates, environment references, and getting-started guides now highlight the
  `embedding_provider` option and the persisted index workflow.
- Migrated the contracts app configuration writer to `tomlkit` so exported wizard bundles
  rely on the same well-supported serializer as the backend services.
- Added regression coverage that exercises every contracts-app configuration field and keeps
  the documentation reference in sync with the wizard options.

### Fixed
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
