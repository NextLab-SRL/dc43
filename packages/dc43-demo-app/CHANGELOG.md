# dc43-demo-app changelog

## [Unreleased]
### Added
- Draft `orders_enriched:3.0.0` contract and curated demo scenario that boosts low amounts,
  stamps placeholder customer segments, and logs contract-status overrides when drafts are allowed.
- Contract status enforcement hooks in the demo pipeline with metadata describing the active policy
  on reads and writes.
- Altair Retail walkthrough with source, modeled, ML, consumer, and KPI data products, including
  sample fixtures, a linear demand forecaster, documentation, and a UI scenario linking the assets.
- Altair Retail UI pages that simulate a marketing activation planner and executive dashboard fed
  by the packaged datasets.
- Altair Retail overview card that re-runs the demo pipeline, embeds a runnable Python snippet,
  illustrates the end-to-end flow with Mermaid, and surfaces catalog crosslinks for products,
  datasets, and contracts directly from the page.
- Altair Retail overview enhancements with dataset-level lineage, zone badges, and a tabbed
  experience section that keeps offer highlights, activation, and dashboard visuals together.
- Altair Retail timeline player that animates months of incidents, releases, and KPI launches,
  pausing on each milestone with deep links into the catalog and faux applications.
- Altair Retail demo timeline insights that surface live metrics, highlight anchors, and
  outbound links to dataset, contract, and product records alongside the catalog tables that
  now open the authoritative pages in new tabs.
- Altair Retail timeline replay that simulates each pipeline step, highlights the exact rule
  failures and reject slices, drives status banners across the activation/dashboard tabs, and
  exposes a richer sample script for presenters to reuse in notebooks.

### Changed
- Updated pipeline scenarios, docs, and tests to reflect the default rejection of non-active
  contracts and the new override workflow for development runs.
- Data-product pipeline enforces the configured expected contract version when resolving
  upstream inputs.
- Altair Retail demo restructured into foundation, star schema, intelligence, experience, and
  analytics data products with internal feature stores, star-schema contracts, and updated UI
  guidance, now exposed via a dedicated navigation entry instead of a pipeline scenario.
- Altair Retail data product tags now emphasise source, modelled, ML, consumer, and aggregated
  personas instead of medallion labels to avoid confusion with the contracts medallion view.
