# dc43 changelog

## [Unreleased]
### Added
- Added a generated Mermaid dependency graph and supporting script so internal package
  relationships are easier to audit during releases.
- Introduced Playwright-based contracts setup wizard UI automation, including
  reusable scenarios, npm scripts (`test:ui`, `test:ui:handoff`), and a bundled
  FastAPI launcher so contributors can run or extend browser tests alongside the
  Python suite.

### Changed
- Updated the release automation to tag the contracts-app and HTTP backend Docker
  images with their package versions, documented the ECR setup flow, and added a
  manual smoke publish path for validating AWS credentials.
- Documented the Gitflow-based branching expectations and clarified how merges from `dev` to `main`
  trigger automated releases in the release guide.
- Relaxed internal package pins in `setup.py` to resolve pip conflicts when installing extras
  that pull from local editable copies.
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
