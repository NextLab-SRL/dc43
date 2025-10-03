# dc43-demo-app changelog

## [Unreleased]
### Added
- Data product roundtrip scenario now reads via ODPS bindings, enriches orders with customer
  lookups, and records input/output statuses across the staged and published datasets.
- Draft `orders_enriched:3.0.0` contract and curated demo scenario that boosts low amounts,
  stamps placeholder customer segments, and logs contract-status overrides when drafts are allowed.
- Contract status enforcement hooks in the demo pipeline with metadata describing the active policy
  on reads and writes.

### Changed
- Updated ODPS seed data so `dp.analytics` highlights its upstream `dp.sales.orders` dependency
  and the demo UI surfaces the dedicated data product listing.
- Updated pipeline scenarios, docs, and tests to reflect the default rejection of non-active
  contracts and the new override workflow for development runs.

### Fixed
- Data product roundtrip scenario now scopes its input read to the active dataset version and
  records failures in the UI, preventing contract validation errors from hiding in logs.
