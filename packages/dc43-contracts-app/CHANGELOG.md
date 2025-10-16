# dc43-contracts-app changelog

## [Unreleased]
### Added
- Pipeline bundle generator now loads integration-provided pipeline stub
  fragments so Spark, Delta Live Tables, and future runtimes ship their own
  helper code without modifying the contracts app.
- Documented contract status guardrails in the integration helper stub and notes so generated Spark
  snippets explain how to opt into draft or deprecated contracts safely.
- Introduced a grouped, accessibility-friendly setup wizard with step badges, reset controls, and a
  live architecture diagram that highlights the components a user selects across contracts,
  products, quality, and governance modules.
- Bundled environment exports now emit TOML configs, Terraform stubs, and launch scripts tailored to
  the selected deployment targets so new installations can jump straight into provisioning.
- Added a pipeline integration module covering Spark and Delta Live Tables runtimes so the wizard
  captures orchestration credentials and the exported helper script shows how to initialise the
  chosen engine alongside the dc43 backends.

### Changed
- The setup architecture view only renders modules that have been explicitly selected or are
  required by user-driven dependencies, preventing unrelated services from appearing in fresh
  configurations.
- Validation results storage now lives in the storage foundations step so Delta, SQL, filesystem,
  and HTTP backends are visible alongside contract and product persistence choices.
- The setup architecture overview groups the pipeline footprint versus remote hosting, surfaces the
  validation results store, and links quality runs back to their persistence target so operators can
  see how governance data flows through the deployment.
- Architecture groupings now distinguish local runtime choices from hosted deployments so the
  diagram no longer lists local Python orchestration under remote hosting and highlights the new
  pipeline integration node.
