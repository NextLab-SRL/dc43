# dc43-service-clients changelog

## [Unreleased]

### Added
- Introduced `load_service_clients` and `load_governance_client` helpers to
  provision local or remote service clients directly from backend
  configuration, providing a single entry point for application bootstrap.

### Changed
- Declared a direct dependency on ``attrs`` so OpenLineage support works out of
  the box for applications that import governance clients without installing
  the full OpenLineage stack separately.
- ODPS helpers now prefer the shared `dc43-core` package when available so
  clients align with the backend implementation without loading service
  modules.
- Bumped the package baseline to ``0.27.0.0`` so Test PyPI validation can
  continue after the ``0.26.0.0`` build was removed upstream.
- Governance clients now expose dataset listing, pipeline activity, and
  validation status helpers so UI consumers can gather run history entirely via
  service APIs.
- Governance clients now expose contract discovery helpers (`get_contract`,
  `latest_contract`, `list_contract_versions`, and
  `describe_expectations`) and include validation payloads in
  `QualityAssessment` responses so integrations can operate solely through the
  governance API.
- Strengthened governance client coverage to ensure registering new
  data-product output bindings surfaces the review-required runtime error,
  mirroring the backend guardrails exercised by the Spark integration tests.
- Governance client context resolution now validates data product version hints
  and source contract requirements so callers receive immediate feedback when
  attempting to bind draft or mismatched products.
- Governance read/write contexts and resolved plans now carry optional data
  product status policies (and the HTTP transport serialises them) so clients
  can opt into draft products or relax enforcement in line with their
  configured status strategies.
- Guarded governance/bootstrap tests with `pytest.importorskip` so the package
  skips cleanly when `dc43_service_backends` is not installed, avoiding
  import-time failures in minimal CI environments.
- Deferred importing the bootstrap module at package import time so the
  `dc43-service-clients` distribution no longer requires
  `dc43_service_backends` just to expose its top-level helpers.
- Governance lineage helpers once again import the OpenLineage client eagerly
  so interface mismatches surface immediately, and the package now declares a
  direct ``openlineage-python`` dependency so downstream installs receive the
  runtime transitively.
- OpenLineage is now exposed via a dedicated `lineage` extra and the lineage
  helpers lazily import the client, allowing minimal installs to skip the
  dependency while still surfacing a descriptive error when the extra is not
  installed.

