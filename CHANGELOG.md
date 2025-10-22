# dc43 changelog

## [Unreleased]
### Added
- Added a generated Mermaid dependency graph and supporting script so internal package
  relationships are easier to audit during releases.
- Introduced Playwright-based contracts setup wizard UI automation, including
  reusable scenarios, npm scripts (`test:ui`, `test:ui:handoff`), and a bundled
  FastAPI launcher so contributors can run or extend browser tests alongside the
  Python suite.
- Added a LangChain + Gradio powered documentation assistant to the dc43 app with
  configuration knobs, deployment guidance, and docs updates so teams can chat with
  the bundled Markdown guides.
- Extended the setup wizard with a documentation assistant module so docs-chat settings
  flow into exported configuration bundles and UI automation scenarios.
- Allowed the demo launcher to accept `--config`/`--env-file` arguments and taught the
  contracts configuration about an inline `docs_chat.api_key` so local runs can avoid
  manual `export` steps when working from private TOML files.
- Added a `dc43-docs-chat-index` CLI so operators can pre-build the documentation
  assistant's FAISS cache using the same configuration that powers the FastAPI app.
- Updated the docs-chat index CLI to fail fast when the assistant configuration
  is incomplete and to print a summary of the indexed sources after a
  successful warm-up.
- Documented every setup wizard module, TOML key, and environment override in a
  single configuration reference under `docs/` so teams can audit available knobs
  without cross-referencing multiple guides.
- Bundled per-module TOML exports in the setup archive and expanded tests to
  ensure every submitted wizard value persists into generated configuration files.

### Changed
- Normalised TOML writers across the demo launcher and backend configuration so
  every export uses the shared serializer, documentation calls the field
  `workspace_url`, and tests assert the generated TOML mirrors wizard input.
- Updated the release automation to tag the contracts-app and HTTP backend Docker
  images with their package versions, documented the ECR setup flow, and added a
  manual smoke publish path for validating AWS credentials.
- Documented the Gitflow-based branching expectations and clarified how merges from `dev` to `main`
  trigger automated releases in the release guide.
- Relaxed internal package pins in `setup.py` to resolve pip conflicts when installing extras
  that pull from local editable copies.
- Added a `docs-chat` extra to the meta package and documented the single editable install
  command so local demos can enable the documentation assistant without juggling multiple
  pip invocations.
- Let the docs assistant target Hugging Face embeddings as well as OpenAI so large
  repositories can be indexed offline; templates and guides now cover the new
  `embedding_provider` option and the persisted cache workflow.
- Defaulted docs chat embeddings to the Hugging Face backend so local warm-ups
  avoid OpenAI rate limits; templates, generated configs, and guides now call
  out how to opt back into the OpenAI embedding API when needed.
- Ensured docs chat answers render in their own chat bubble while the
  processing log appears in a separate collapsible message so streaming
  updates no longer hide the response.
- Clarified docs-chat installation notes so contributors avoid mixing the meta extra with
  direct `dc43-contracts-app[docs-chat]` installs, which pip treats as conflicting
  requirements in a single environment.
- Documented that `DC43_CONTRACTS_APP_CONFIG` must be exported for the docs assistant to
  load custom TOML files and clarified that `docs_chat.api_key_env` stores the name of the
  secret-bearing environment variable.
- Updated docs and templates to highlight the new launcher flags, `.env` support, and the
  optional `docs_chat.api_key` field for storing secrets outside environment variables.
- Logged the resolved contracts-app configuration path and docs-chat status during
  `dc43-demo` startup to make local troubleshooting easier.
- Limited the docs chat auto-discovered code directories to the dc43 checkout so
  editable installs no longer scan sibling projects and hit embedding rate limits.
- Taught the docs chat configuration loader to accept hyphenated `code-path`
  keys and updated the offline index CLI to echo the resolved sources and
  providers before warm-up so misconfigurations are easier to spot.
- Streamed docs chat progress as concise updates, kept answers in their own
  chat bubble, and moved the detailed step log into a separate collapsible
  message so results stay visible without scrolling through status updates.
- Tuned the docs assistant prompt so answers lean on retrieved Markdown, cite relevant
  files, provide actionable integration steps, and explicitly surface helper APIs when
  users ask how to wire dc43 into pipelines.
- Expanded the Spark local quickstart with concrete `read_with_contract` and status
  strategy examples so developers can drop the integration layer into existing jobs
  without hunting through other guides.
- Extended the docs assistant to index repository source files, honour configurable
  `docs_chat.code_paths`, and pass optional OpenAI reasoning effort hints so complex
  questions return context-rich guidance grounded in both docs and code.
- Added a dc43-only guardrail to the docs assistant so off-topic prompts receive a polite
  reminder that the chat surface focuses on project setup and usage guidance.
- Kicked off docs assistant warm-up in the background during application startup, cached
  the vector index between runs, and now relay the queued warm-up progress to both the
  UI and server logs (including the explicit warm-up wait message exposed via the API
  `steps` field) so users see each stage even when the cache build started before their
  first question, without delaying service start-up.
- Migrated the ODCS/ODPS helpers into the backend package and kept the meta
  distribution as a thin compatibility layer to eliminate dependency cycles
  when installing integration extras.
- Simplified the CI workflow to avoid duplicate push/PR runs and ensured the contracts app
  job installs SQLAlchemy so its test suite resolves backend dependencies.
- Renamed the root CI matrix entry to `meta`, moved the demo and backend integration tests
  into their dedicated packages, and added a demo-app matrix job so each distribution owns
  its test suite without duplicating runs.
- Ensured the demo-app CI lane installs `open-data-contract-standard` so the demo pipeline
  and UI tests can exercise the backend helpers without missing dependencies.
- Replaced the hand-written TOML emitters with `tomlkit` so configuration bundles reuse a
  well-supported serializer and match the loaders' expectations across the setup wizard,
  contracts UI, and backend services.
- Expanded the service backend configuration guide with Unity Catalog workspace examples so
  Delta deployments spell out how to capture hosts, tokens, and CLI profiles in TOML.

### Fixed
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
