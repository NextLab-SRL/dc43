# dc43-service-backends changelog

## [Unreleased]
### Added
- Data product backends now consume the shared ODPS helpers from
  `dc43-service-clients` so they can operate without the core package during
  isolated builds.
- Added `DataProductRegistrationResult` metadata so callers can detect when ODPS
  registrations create new drafts.
- Introduced `FilesystemDataProductServiceBackend` which persists ODPS documents
  as JSON compatible with the standard schema for local and CI environments.
- Added `CollibraDataProductServiceBackend` alongside stub and HTTP adapters so
  deployments backed by Collibra can expose ODPS APIs without custom glue code.
- Added a Unity Catalog tagging bridge controlled by the `[unity_catalog]`
  configuration so backend link operations update Databricks tables without
  changing Spark pipelines.
- Documented and tested Unity Catalog tagging when the governance backend is
  wired to remote contract/data product services, ensuring remote databases such
  as PostgreSQL or Azure Files remain compatible.
### Changed
- Unity Catalog tagging now runs through pluggable governance hooks so service
  and client interfaces stay technology agnostic while still supporting
  Databricks-specific metadata updates.
