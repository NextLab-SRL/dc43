# dc43-service-backends changelog

## [Unreleased]
### Added
- Added `DataProductRegistrationResult` metadata so callers can detect when ODPS
  registrations create new drafts.
- Introduced `FilesystemDataProductServiceBackend` which persists ODPS documents
  as JSON compatible with the standard schema for local and CI environments.
- Added `CollibraDataProductServiceBackend` alongside stub and HTTP adapters so
  deployments backed by Collibra can expose ODPS APIs without custom glue code.
