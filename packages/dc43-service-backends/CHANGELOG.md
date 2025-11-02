# dc43-service-backends changelog

## [Unreleased]

### Changed
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

### Fixed
- SQL governance activity lookups now include the dataset identifier and version
  from the table columns when payloads omit those fields, ensuring clients can
  enumerate available dataset versions even for legacy records.
- SQL governance status lookups now tolerate duplicate historical rows by
  selecting the most recent payload, preventing `MultipleResultsFound` errors
  when legacy tables contain redundant entries.
- ODPS serialisation raises a descriptive error when non data-product objects
  (for example, Open Data Contracts) are passed to the helper, steering callers
  towards the correct client API instead of surfacing an attribute error.

