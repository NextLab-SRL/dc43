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
- Introduced `DeltaDataProductServiceBackend` plus corresponding configuration
  knobs so Databricks deployments can persist ODPS payloads in Unity Catalog
  tables instead of DBFS folders.
- Added bootstrap helpers (`build_backends`, `build_contract_store`,
  `build_data_product_backend`) that translate the TOML configuration into
  concrete backends for notebooks, services, or tests.
- Introduced `SQLContractStore` so deployments can persist contracts in
  relational databases (PostgreSQL, MySQL, SQL Server, SQLite) via SQLAlchemy.
- Documented SQL-backed deployment guidance and configuration templates covering
  managed databases on Azure, AWS, and other clouds.
- Terraform blueprints for Azure Container Apps and AWS Fargate now accept
  `contract_store_mode = "sql"` to inject DSNs and skip shared storage mounts.
- Documented and tested Unity Catalog tagging when the governance backend is
  wired to remote contract/data product services, ensuring remote databases such
  as PostgreSQL or Azure Files remain compatible.
- Governance configuration now accepts
  `governance.dataset_contract_link_builders` (and the
  `DC43_GOVERNANCE_LINK_BUILDERS` environment variable) so deployments can load
  custom dataset–contract link hooks without editing the service code.
### Changed
- Unity Catalog tagging now runs through pluggable governance hooks so service
  and client interfaces stay technology agnostic while still supporting
  Databricks-specific metadata updates.
- The HTTP webapp delegates hook assembly to a governance bootstrap module, so
  Databricks integrations and future extensions live in dedicated files rather
  than being hard-coded in the entrypoint.
- Governance bootstrapper now resolves hook builders from import strings to keep
  Unity Catalog logic out of the default wiring and make alternative
  implementations first-class configuration options.
- Contract and data product store configuration now accept `table` entries and
  `data_product` settings so Unity Catalog tables or alternative storage layers
  can be selected without modifying the service code.
- Contract store configuration adds `dsn`/`schema` keys plus corresponding
  environment overrides for SQL deployments.
- Configuration loaders respect `DC43_CONTRACT_STORE_TYPE`, enabling IaC tools
  to switch backends without editing TOML files, and the Delta store reuses the
  SQL serialisation helper to keep schemas aligned.
