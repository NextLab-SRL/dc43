# dc43-service-clients changelog

## [Unreleased]
### Added
- Client APIs now surface `DataProductRegistrationResult` so orchestrators can
  fail pipelines when ODPS input/output registrations create drafts.
- Updated the remote adapter to consume the new backend response shape including
  the registration metadata.
