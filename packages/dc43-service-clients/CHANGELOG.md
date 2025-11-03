# dc43-service-clients changelog

## [Unreleased]

### Added
- Introduced `load_service_clients` and `load_governance_client` helpers to
  provision local or remote service clients directly from backend
  configuration, providing a single entry point for application bootstrap.

### Changed
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
- Guarded governance/bootstrap tests with `pytest.importorskip` so the package
  skips cleanly when `dc43_service_backends` is not installed, avoiding
  import-time failures in minimal CI environments.
- Deferred importing the bootstrap module at package import time so the
  `dc43-service-clients` distribution no longer requires
  `dc43_service_backends` just to expose its top-level helpers.

