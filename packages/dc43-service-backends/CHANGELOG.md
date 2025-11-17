# dc43-service-backends changelog

## [Unreleased]

### Changed
- Core ODCS/ODPS helpers now live in the shared `dc43-core` package and this
  distribution imports them directly, ensuring all runtimes share the same
  implementation without maintaining duplicate copies.
- Raised the `dc43-core` dependency floor to 0.27.0.0 so rc rewrites keep the
  shared helper package in sync with the service backends during Test PyPI
  validation.
- Bumped the package baseline to ``0.27.0.0`` so Test PyPI validation can
  continue after the ``0.26.0.0`` build was removed upstream.
- Governance backends and stores now surface `list_datasets` and pipeline
  activity/status lookups so UI clients can assemble dataset history directly
  from the service without relying on demo-specific record stores.
- Local governance backends now expose contract resolution helpers and include
  underlying validation payloads when returning `QualityAssessment` objects so
  clients relying solely on the governance layer retain access to detailed data
  quality results.
- Governance storage now guards the SQL backend import so environments without
  SQLAlchemy can still import the package and run client-only tests without
  pulling in optional dependencies.
- Governance registration hooks now raise review-required errors when data
  product input/output registrations create new drafts, matching the behaviour
  of the legacy Spark contract helpers so governance-first pipelines surface the
  same guardrails.
- Governance context resolution now enforces data product version selectors and
  source contract requirements so reads and writes block on draft products or
  mismatched upstream contracts before producing new registrations.
- Governance backends now honour caller-provided data product status policies,
  allowing draft versions to resolve when the context explicitly permits them
  while continuing to block unexpected states by default during resolution and
  registration.
- The test extra now depends on `dc43-service-clients[lineage]` so lineage
  helpers continue to run during backend tests even though OpenLineage is an
  optional dependency.

### Fixed
- Delta governance stores now supply explicit schemas when persisting status,
  link, and activity records so Spark no longer fails to infer field types when
  optional values (such as lineage payloads) are null.
- Governance stores now pull metric observations from validation detail
  payloads when the explicit ``metrics`` attribute is empty so SQL/Delta/HTTP
  backends continue populating `dq_metrics` tables even when upstream
  validations serialise observations only inside `dq_status` rows.
- Hardened the governance status matrix endpoint so mixed payload types (for
  example, pre-encoded validation dictionaries) no longer trigger 500 errors
  when UI clients request batched status snapshots.
- SQL governance stores now fall back gracefully when legacy activity tables
  are missing the `updated_at` column, keeping batched status lookups and
  inserts working without manual schema migrations.
- SQL governance activity lookups now include the dataset identifier and version
  from the table columns when payloads omit those fields, ensuring clients can
  enumerate available dataset versions even for legacy records.
- SQL governance status lookups now tolerate duplicate historical rows by
  selecting the most recent payload, preventing `MultipleResultsFound` errors
  when legacy tables contain redundant entries.
- ODPS serialisation raises a descriptive error when non data-product objects
  (for example, Open Data Contracts) are passed to the helper, steering callers
  towards the correct client API instead of surfacing an attribute error.
- Delta-backed contract and data product stores now compare version strings with
  suffix-aware keys so rc/dev builds sort correctly without triggering Spark
  cast errors when resolving the latest entries.
- Delta-backed stores now ignore empty version markers and rank ``draft``
  suffixes as pre-releases, ensuring ``latest`` resolution succeeds even when
  historical rows contain placeholder records.
- Data product registration during read/write activity reloads explicit binding
  versions before enforcement so requested drafts or historical releases are
  validated rather than the latest product returned by the registration helper.
- Governance write registration skips automatic data product evolution when the
  binding pins an explicit version, preventing Databricks demos (and other
  pipelines that manage releases manually) from tripping review-required errors
  after they attach custom port metadata such as physical locations.
- Governance read registration now mirrors that behaviour for pinned inputs so
  consumers that attach lineage metadata or source references don't create new
  drafts when they intentionally target an existing release.
- Delta governance stores now delete existing status rows for a dataset/version
  before recording a new verdict, preventing duplicate compatibility entries in
  Databricks tables and in the contracts application history views.

