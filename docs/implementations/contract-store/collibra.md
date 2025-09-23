# Collibra Contract Store

`CollibraContractStore` delegates contract persistence and lifecycle to
Collibra Data Products. It is backed by the
`HttpCollibraContractGateway`, which wraps Collibra's REST APIs for Data
Product ports and contract versions.

## Characteristics

* Uses Collibra as the source of truth for contract status (`Draft`,
  `Validated`, `Deprecated`, …).
* Supports filtering by workflow status when resolving the latest
  contract (for example `latest(..., status_filter="Validated")`).
* Shares stewardship workflows with other Collibra artefacts so data
  product owners can review drafts and approvals in a single tool.

Refer to the [Collibra integration guide](../collibra/contract-integration.md)
for architecture diagrams, webhook flows, and operational
considerations. The same gateway also participates in the data-quality
governance stories documented there.
