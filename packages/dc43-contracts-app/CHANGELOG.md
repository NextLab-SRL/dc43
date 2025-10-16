# dc43-contracts-app changelog

## [Unreleased]
### Added
- Setup bundle archives now ship a ready-to-run `requirements.txt`, cross-platform
  environment bootstrap scripts, and Docker build/publish helpers so teams can
  stand up virtual environments or prebuilt images without manual packaging.
- Setup bundle exports now include full Spark and Delta Live Tables example
  projects (code modules, README, and ops helpers) alongside the
  `examples/pipeline_stub.py` entrypoint so teams can start from a realistic
  integration scaffold.
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
- Added a documentation chat experience powered by LangChain and Gradio, including configuration
  defaults, a Gradio-mounted UI, and a REST endpoint for programmatic access.
- Introduced a documentation assistant module in the setup wizard so exported bundles capture the
  docs chat configuration alongside other deployment metadata.

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
- Rebranded the UI to the "dc43 app", added a docs chat navigation entry, and documented the
  configuration knobs required to enable the assistant in deployments.
- Clarified installation guidance for the docs chat assistant so source checkouts and PyPI
  consumers know which pip command enables the optional dependencies.
- Documented that mixing the meta package demo extra with a direct
  `dc43-contracts-app[docs-chat]` install in the same environment leads to pip conflicts, and
  pointed contributors at the single-command workflow.
