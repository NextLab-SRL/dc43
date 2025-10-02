# dc43-service-clients changelog

## [Unreleased]
### Added
- Bundled ODPS helpers so the package can run its tests without the core
  distribution installed and to provide a single import path for downstream
  service components.
- Client APIs now surface `DataProductRegistrationResult` so orchestrators can
  fail pipelines when ODPS input/output registrations create drafts.
- Updated the remote adapter to consume the new backend response shape including
  the registration metadata.
