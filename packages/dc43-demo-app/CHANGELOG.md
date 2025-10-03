# dc43-demo-app changelog

## [Unreleased]
### Added
- Data product roundtrip scenario now reads via ODPS bindings, enriches orders with customer
  lookups, and records input/output statuses across the staged and published datasets.
- Curated data product roundtrip scenario that pins the input slice to 2024-01-01 to produce an OK
  outcome alongside the failure example.
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
- Pipeline runs page shows ODPS failures in the violations column, enriches DQ details with binding
  information, and separates contract scenarios from data product flows for quicker navigation.
- Hardened dataset version marker creation so curated roundtrip runs succeed even when symlinked
  targets already exist during registration.
- Captured data product input validation failures in their own DQ sections, marked downstream
  stages as skipped, and avoided duplicating schema errors in the output summary.
