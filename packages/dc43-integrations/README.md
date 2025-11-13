# dc43-integrations

The integration packages connect the dc43 service contracts to runtime platforms. Today the
Spark adapter ships here, and additional adapters can live alongside it while depending only
on the shared client contracts.

## Installation

Governance adapters depend on optional runtimes so deployments can opt in only when they
need a specific integration:

- `spark` installs the PySpark runtime.
- `lineage` installs OpenLineage helpers for emitting run events.
- `telemetry` installs the OpenTelemetry SDK used by governance span recording.

For example, to provision the full Spark governance stack (matching the configuration
used in CI) install all extras:

```bash
pip install "dc43-integrations[spark,lineage,telemetry]"
```
