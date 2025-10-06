# dc43-demo-app changelog

## [Unreleased]
### Added
- Draft `orders_enriched:3.0.0` contract and curated demo scenario that boosts low amounts,
  stamps placeholder customer segments, and logs contract-status overrides when drafts are allowed.
- Contract status enforcement hooks in the demo pipeline with metadata describing the active policy
  on reads and writes.
- Altair Retail walkthrough with source, modeled, ML, consumer, and KPI data products, including
  sample fixtures, a linear demand forecaster, documentation, and a UI scenario linking the assets.

### Changed
- Updated pipeline scenarios, docs, and tests to reflect the default rejection of non-active
  contracts and the new override workflow for development runs.
- Data-product pipeline enforces the configured expected contract version when resolving
  upstream inputs.
- Altair Retail demo restructured into foundation, star schema, intelligence, experience, and
  analytics data products with internal feature stores, star-schema contracts, and updated UI
  guidance.
