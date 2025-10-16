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

### Changed
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
- Clarified docs-chat installation notes so contributors avoid mixing the meta extra with
  direct `dc43-contracts-app[docs-chat]` installs, which pip treats as conflicting
  requirements in a single environment.
- Documented that `DC43_CONTRACTS_APP_CONFIG` must be exported for the docs assistant to
  load custom TOML files and clarified that `docs_chat.api_key_env` stores the name of the
  secret-bearing environment variable.
- Updated docs and templates to highlight the new launcher flags, `.env` support, and the
  optional `docs_chat.api_key` field for storing secrets outside environment variables.
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

### Fixed
- Updated the `dc43-demo` launcher to merge any exported `DC43_CONTRACTS_APP_CONFIG`
  file into the generated workspace configuration so docs-chat overrides stay
  active instead of being reset to the default template.
