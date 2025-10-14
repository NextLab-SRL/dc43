# dc43-contracts-app changelog

## [Unreleased]
### Added
- Documented contract status guardrails in the integration helper stub and notes so generated Spark
  snippets explain how to opt into draft or deprecated contracts safely.
- Introduced a grouped, accessibility-friendly setup wizard with step badges, reset controls, and a
  live architecture diagram that highlights the components a user selects across contracts,
  products, quality, and governance modules.
- Bundled environment exports now emit TOML configs, Terraform stubs, and launch scripts tailored to
  the selected deployment targets so new installations can jump straight into provisioning.

### Changed
- The setup architecture view only renders modules that have been explicitly selected or are
  required by user-driven dependencies, preventing unrelated services from appearing in fresh
  configurations.
- Validation results storage now lives in the storage foundations step so Delta, SQL, filesystem,
  and HTTP backends are visible alongside contract and product persistence choices.
