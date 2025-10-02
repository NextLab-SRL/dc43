# dc43-service-clients changelog

## [Unreleased]
### Added
- Bundled ODPS helpers so the package can run its tests without the core
  distribution installed and to provide a single import path for downstream
  service components.
- Hardened the ODPS helper import guard so standalone installs no longer raise
  ``ModuleNotFoundError`` when the root ``dc43`` package is absent.
- Client APIs now surface `DataProductRegistrationResult` so orchestrators can
  fail pipelines when ODPS input/output registrations create drafts.
- Updated the remote adapter to consume the new backend response shape including
  the registration metadata.
- Added a lightweight `LocalDataProductServiceBackend` testing stub so the
  package test suite no longer depends on the service backend distribution.

### Fixed
- Decoupled the data product clients from the backend package so importing the
  protocol and client helpers no longer requires installing the service
  backends distribution.
