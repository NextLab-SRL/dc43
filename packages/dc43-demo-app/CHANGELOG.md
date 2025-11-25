# dc43-demo-app changelog

## [Unreleased]

### Changed
- Bumped the package baseline to ``0.27.0.0`` so Test PyPI validation can
  continue after the ``0.26.0.0`` build was removed upstream.
- Removed the `register_dataset_records` helper and manual history seeding;
  demo pipelines, streaming scenarios, and tests now rely on governance service
  calls to produce dataset history, with fixtures resetting state between runs.
- Register the demo dataset record loader/saver through the contracts service
  hooks so the UI continues to surface run history without relying on the
  contracts package to provision its own record store.
- Pipeline and streaming demo scenarios now call the governance read/write
  helpers with `GovernanceSparkReadRequest`/`GovernanceSparkWriteRequest`
  payloads so presenters only need the governance client to resolve contracts,
  data product bindings, and status reporting.
- Demo DLT walkthroughs now import the renamed
  ``governed_expectations``/``governed_table`` helpers so notebook snippets and
  generated copy match the governance-first annotations.
- Rely on the service-client dependency graph to pull in ``openlineage-python``
  so the demo no longer duplicates the lineage runtime requirement.
- The demo record loader now accepts ``dataset_id``/``dataset_version`` filters
  so the contracts UI and demo helpers can request a single dataset run without
  re-reading the entire JSON registry.
- Running ``python -m dc43_demo_app.runner`` now launches the demo backend,
  contracts app, and UI with clear startup logs so local module execution keeps
  the full stack online for seeding and screenshots.

