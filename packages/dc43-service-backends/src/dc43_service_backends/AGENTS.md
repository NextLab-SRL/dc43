# AGENTS Instructions

## Configuration plumbing
- When introducing new configuration keys for the service backends, update **all** touchpoints in `config.py`: dataclasses, `load_config`, environment overrides, and `config_to_mapping` helpers.
- Mirror the new option in the documentation (`docs/service-backends-configuration.md`), sample templates under `docs/templates`, and the relevant changelog entries.
- Add or expand tests in `packages/dc43-service-backends/tests/test_config.py` (and other suites when needed) to prove the field is parsed from files and environment variables.
- Ensure bootstrap code and store builders consume the new config so values actually propagate to runtime components.
