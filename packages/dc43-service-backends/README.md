# dc43-service-backends

Backend-facing components that fulfill the dc43 service contracts live in this
package. Install it alongside `dc43-service-clients` when wiring custom storage,
governance, or quality enforcement backends.

## Configuration

The service backend HTTP application reads its settings from TOML files. Refer
to [docs/service-backends-configuration.md](../../docs/service-backends-configuration.md)
for the supported options—including filesystem, SQL, Delta, Collibra stub, and
Collibra HTTP contract stores—alongside editable templates. The same
configuration file now controls the data-quality delegate (`local` or `http`),
pluggable execution engines (native, Great Expectations, Soda), and the
governance store (memory, filesystem, SQL, Delta, or HTTP).
