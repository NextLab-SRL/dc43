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

### Fixed
- `StrictWriteViolationStrategy` now reuses the wrapped strategy's contract status
  allowances so strict enforcement respects custom governance policies.
