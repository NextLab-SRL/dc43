# dc43-integrations changelog

## [Unreleased]
### Added
- Enforced Open Data Contract status guardrails in the Spark read/write helpers with
  configurable policies that default to rejecting non-active contracts and expose
  overrides through the existing read and write strategies.
- Documented the new contract status options across the demo pipeline, integration
  helper, and docs to help teams opt into draft or deprecated contracts for
  development scenarios while keeping production defaults strict.

### Fixed
- `StrictWriteViolationStrategy` now reuses the wrapped strategy's contract status
  allowances so strict enforcement respects custom governance policies.
