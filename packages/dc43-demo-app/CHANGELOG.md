# dc43-demo-app changelog

## [Unreleased]
### Added
- DLT execution mode for the orders_enriched scenario with a shared
  `run_dlt_pipeline` helper, LocalDLTHarness integration, and regression tests
  that exercise the contract decorators locally.
- Structured Streaming scenarios that demonstrate contract-enforced reads and
  writes directly inside the pipeline demo, plus a `dc43_demo_app.streaming`
  helper for triggering the healthy, reject-routing, and schema-drift flows
  from code.
- Streaming scenario timeline visualisation with micro-batch cards, live metric
  snapshots, Mermaid diagrams, scenario copy that documents source cadence and
  dataset roles, and a tutorial walkthrough that references the new UI cues.
- Streaming live-run card that streams server-sent events, animates the
  micro-batch badges while the pipeline runs, and exposes a streaming progress
  API for embedding the contract-aware timeline in other presenters.
- Draft `orders_enriched:3.0.0` contract and curated demo scenario that boosts low amounts,
  stamps placeholder customer segments, and logs contract-status overrides when drafts are allowed.
- Contract status enforcement hooks in the demo pipeline with metadata describing the active policy
  on reads and writes.
- Altair Retail walkthrough with source, modeled, ML, consumer, and KPI data products, including
  sample fixtures, a linear demand forecaster, documentation, and a UI scenario linking the assets.
- Altair Retail UI pages that simulate a marketing activation planner and executive dashboard fed
  by the packaged datasets.
- Altair Retail overview card that re-runs the demo pipeline, anchors sections with a sticky
  navigation rail, illustrates the end-to-end flow with Mermaid, and surfaces catalog crosslinks for products,
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
- Pipeline scenario details now run Spark or DLT modes in place using asynchronous
  progress indicators, Bootstrap toasts for completion status, and the JSON
  pipeline endpoint instead of redirecting back to the scenario list.
- The scenario list now exposes Spark and DLT run buttons for every contract walkthrough,
  dispatches runs via AJAX so the table stays in view, and refreshes status cells once
  the toast notification confirms completion.
- Contract-driven pipeline scenarios now embed tailored DLT notebook snippets and
  the DLT-only walkthrough mirrors the same annotation-driven code shown in the
  UI guides.
- DLT runs now resolve the ``dlt`` module through ``ensure_dlt_module``, record
  whether the stub fallback was used, and keep the demo executable even when
  ``databricks-dlt`` is not installed locally.
- Updated pipeline scenarios, docs, and tests to reflect the default rejection of non-active
  contracts and the new override workflow for development runs.
- Data-product pipeline enforces the configured expected contract version when resolving
  upstream inputs.
- Altair Retail demo restructured into foundation, star schema, intelligence, experience, and
  analytics data products with internal feature stores, star-schema contracts, and updated UI
  guidance, now exposed via a dedicated navigation entry instead of a pipeline scenario.
- Altair Retail data product tags now emphasise source, modelled, ML, consumer, and aggregated
  personas instead of medallion labels to avoid confusion with the contracts medallion view.
- Altair Retail overview now folds the catalog tables into the story tabs, trims the navigation
  rail, and removes the KPI preview so the flow, lineage, and timeline visuals remain the focus.
- Altair Retail story tabs now render Mermaid diagrams inline and drop the catalog quick-link
  banners so the timeline, flow, and lineage visuals stay front and centre.
- Streaming demo scenarios now record per-micro-batch dataset versions, filter noisy metric
  warnings from streaming reads, and surface direct links to the input and reject datasets on the
  pipeline detail page.
- Streaming reject walkthrough now alternates healthy and warning batches, keeps the reject sink
  ungoverned while recording its filesystem path, and updates the UI copy so the catalog stops
  creating contracts or drafts for the quarantined rows while still exposing them for remediation.
- Streaming run history now focuses on the primary contract slice, surfaces the ungoverned reject
  path, orders the versions by recency, and refreshes the scenario diagram so the governance
  hand-offs highlight which assets are contract-backed versus tracked without a contract.
- Adopted the demo pipeline, UI, and streaming regression tests into the package so its
  distribution validates the bundled scenarios directly during CI runs.

### Fixed
- Respect exported `DC43_CONTRACTS_APP_CONFIG` files when launching `dc43-demo`
  by merging their overrides (such as `[docs_chat]` settings) into the generated
  workspace configuration instead of overwriting them.
- Prevent the Altair Retail timeline replay from crashing when comparing timezone-aware
  inventory snapshots with the freshness reference checkpoint.
- Ensure the Altair Retail flow and dataset lineage tabs render their Mermaid diagrams reliably
  inside the story tabs instead of leaving the graph source text visible.
- Avoid serialising active `StreamingQuery` handles when recording streaming scenario results so
  the reject-routing walkthrough can persist dataset history without errors.
- Restore filesystem aliases for timestamped streaming versions so dataset previews resolve the
  governed slices on platforms that support colon-separated folder names.
- Install the ODCS reference implementation during CI runs so the demo suite's service-client
  and backend imports succeed without additional bootstrap steps.
