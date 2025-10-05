# dc43-integrations changelog

## [Unreleased]
### Added
- Enforced Open Data Contract status guardrails in the Spark read/write helpers with
  configurable policies that default to rejecting non-active contracts and expose
  overrides through the existing read and write strategies.
- Documented the new contract status options across the demo pipeline, integration
  helper, and docs to help teams opt into draft or deprecated contracts for
  development scenarios while keeping production defaults strict.
- Added contract- and data-product-specific helpers
  (`read_from_contract`, `write_with_contract_id`, `read_from_data_product`,
  `write_to_data_product`) that resolve ODPS contracts automatically and abort
  pipelines when port registration produces a draft version.
- `read_from_data_product` accepts an `expected_contract_version` argument so
  data-product reads can pin the upstream schema revision.

### Removed
- Removed the Databricks `UnityCatalogPublisher` wrapper now that Unity Catalog
  tagging is handled transparently by the governance backend configuration.

### Fixed
- `StrictWriteViolationStrategy` now reuses the wrapped strategy's contract status
  allowances so strict enforcement respects custom governance policies.
- Spark writes that overwrite their source path now checkpoint the aligned dataframe
  before executing so contract-enforced data product registrations succeed without
  triggering spurious "file not found" failures, and `write_to_data_product`
  accepts explicit contract identifiers for the final stage of DP pipelines.
