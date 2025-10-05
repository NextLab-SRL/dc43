# Configuring TOML-backed dc43 services

Multiple dc43 packages rely on TOML files for runtime configuration. This guide
covers two of them:

* **`dc43-service-backends`** – the FastAPI application that exposes contract
  and governance backends.
* **`dc43-contracts-app`** – the UI for drafting and publishing contracts.

Both packages ship with defaults, but production deployments usually need to
point at specific backends, catalogs, or workspace folders.

## Where configuration is loaded from

Each application looks for configuration in the following order:

| Package | Loader | Environment override | Packaged default |
| ------- | ------ | -------------------- | ---------------- |
| `dc43-service-backends` | `dc43_service_backends.config.load_config()` | `DC43_SERVICE_BACKENDS_CONFIG` | `dc43_service_backends/config/default.toml` |
| `dc43-contracts-app` | `dc43_contracts_app.config.load_config()` | `DC43_CONTRACTS_APP_CONFIG` | `dc43_contracts_app/config/default.toml` |

Invalid or unreadable TOML is ignored and the packaged defaults are used
instead.

### Environment variables with higher precedence

The loaders also honour targeted environment variables so you can override a
single field without editing the TOML file:

* Service backends:
  * `DC43_CONTRACT_STORE` – overrides the filesystem path or stub base path used
    by the active contract store.
  * `DC43_BACKEND_TOKEN` – overrides the bearer token required by the HTTP API.
* Contracts app:
  * `DC43_CONTRACTS_APP_WORK_DIR` / `DC43_DEMO_WORK_DIR` – overrides the
    workspace directory.
* Additional backend specific overrides exist for the contracts UI (see
  [Contracts app configuration](#contracts-app-configuration)).

## Service backend configuration schema

The service backend configuration supports three core tables:
`contract_store`, `data_product`, and `auth`.

```toml
[contract_store]
type = "filesystem"
root = "./contracts"

[data_product]
type = "memory"

[auth]
token = "change-me"
```

### Choosing a contract store implementation

Set `contract_store.type` to pick which backend the FastAPI application should
expose. Supported values are:

| Type | Description |
| ---- | ----------- |
| `filesystem` | Stores contracts on the local filesystem using `FSContractStore`. |
| `delta` | Persists contracts in a Delta table or Unity Catalog object via `DeltaContractStore`. Requires `pyspark`. |
| `collibra_stub` | Wraps the in-repo Collibra stub adapter, useful for integration tests and demos that emulate Collibra workflows locally. |
| `collibra_http` | Connects to a real Collibra Data Products deployment through `HttpCollibraContractAdapter`. |

The remaining keys under `contract_store` configure the selected backend.

#### Filesystem contract store

| Key | Type | Applies to | Description |
| --- | ---- | ---------- | ----------- |
| `root` | string | `filesystem` | Absolute or relative path to the directory that stores contracts. Defaults to `./contracts` when omitted. Paths may include `~` to reference the current user's home directory. |

#### Delta contract store

| Key | Type | Description |
| --- | ---- | ----------- |
| `table` | string | Fully-qualified Unity Catalog table name used to persist contracts. Required when running inside Databricks workspaces. |
| `base_path` | string | Optional Delta path backing the table when Unity Catalog is not available. Mutually exclusive with `table`. |

Set `type = "delta"` to activate the Delta-backed store. The service attempts to
import `pyspark` and uses `SparkSession.builder.getOrCreate()` to access the
workspace catalog. Provide either `table` or `base_path`; if both are defined
the table takes precedence.

#### Collibra stub contract store

| Key | Type | Description |
| --- | ---- | ----------- |
| `base_path` | string | Optional location for the stub's on-disk cache. When omitted a temporary directory is created automatically. |
| `default_status` | string | Workflow status applied when new contracts are upserted (defaults to `Draft`). |
| `status_filter` | string | Optional workflow status filter applied to version listings (e.g. `Validated`). |
| `catalog` | table | Mapping of contract identifiers to `{ data_product, port }` pairs that represent Collibra catalog entries. |

Define catalog entries using dotted tables:

```toml
[contract_store.catalog."product-quality"]
data_product = "data-products/customer"
port = "gold-quality"
```

#### Collibra HTTP contract store

| Key | Type | Description |
| --- | ---- | ----------- |
| `base_url` | string | **Required.** Base URL of the Collibra environment (e.g. `https://collibra.example.com`). |
| `token` | string | Optional bearer token used for authenticating against Collibra's REST API. |
| `timeout` | float | Request timeout in seconds (defaults to `10.0`). |
| `contracts_endpoint_template` | string | Overrides the REST path template when your Collibra instance customises endpoints. The default matches `/rest/2.0/dataproducts/{data_product}/ports/{port}/contracts`. |
| `default_status` | string | Workflow status applied when upserting contracts. |
| `status_filter` | string | Optional workflow status filter applied to listings. |
| `catalog` | table | Mapping of contract identifiers to Collibra `{ data_product, port }` pairs as described above. |

### Configuring the data product store

`[data_product]` controls how the service persists Open Data Product Standard
documents. Supported types are:

| Type | Description |
| ---- | ----------- |
| `memory` | Stores definitions in-memory. Useful for local demos. |
| `filesystem` | Persists each version as JSON files compatible with the ODPS schema. |
| `delta` | Persists products in a Delta table or Unity Catalog object via `DeltaDataProductServiceBackend`. Requires `pyspark`. |
| `collibra_stub` | Leverages the Collibra stub adapter to emulate remote data product catalogues. |

Common keys include:

| Key | Type | Applies to | Description |
| --- | ---- | ---------- | ----------- |
| `root` | string | `filesystem`, `collibra_stub` | Root directory used for JSON persistence or stub caches. |
| `table` | string | `delta` | Fully-qualified Unity Catalog table used for ODPS payloads. |
| `base_path` | string | `delta` | Delta path backing the table when Unity Catalog is unavailable. |

Like the contract store, the Delta option automatically initialises the table and
expects either `table` or `base_path` to be defined. Switching the type to a
remote backend keeps the service compatible with managed stores such as
PostgreSQL or Azure Files.

#### Authentication

The `auth` table lets you require clients to supply a bearer token with each
request.

| Key | Type | Description |
| --- | ---- | ----------- |
| `token` | string | Optional token value compared against the `Authorization: Bearer <token>` header. Set to an empty string to disable token authentication, which is useful for local development. |

## Contracts app configuration

The contracts UI reads two tables: `workspace` and `backend`.

```toml
[workspace]
root = "~/contracts"

[backend]
mode = "remote"
base_url = "https://service-backends.internal"

[backend.process]
host = "0.0.0.0"
port = 8010
log_level = "info"
```

| Section | Key | Description |
| ------- | --- | ----------- |
| `workspace` | `root` | Directory where draft contracts and uploads are stored. Mirrors `DC43_CONTRACTS_APP_WORK_DIR`. |
| `backend` | `mode` | Select `embedded` to launch the service backends process locally, or `remote` to connect to an existing deployment. Environment override: `DC43_CONTRACTS_APP_BACKEND_MODE`. |
| `backend` | `base_url` | Base URL of the remote backend when `mode = "remote"`. Mirrors `DC43_CONTRACTS_APP_BACKEND_URL`. |
| `backend.process` | `host` | Hostname to bind when running the embedded backend (`DC43_CONTRACTS_APP_BACKEND_HOST`). |
| `backend.process` | `port` | TCP port for the embedded backend (`DC43_CONTRACTS_APP_BACKEND_PORT`). |
| `backend.process` | `log_level` | Optional log level forwarded to the embedded backend (`DC43_CONTRACTS_APP_BACKEND_LOG`). |

## Templates

Editable templates live under `docs/templates/`:

* `dc43-service-backends.toml` – examples for filesystem and Collibra-backed
  service configurations.
* `dc43-contracts-app.toml` – contracts UI settings that cover both embedded and
  remote backend modes.

Copy the relevant file to a writable location, adjust the values to match your
environment, and point the applications at the resulting TOML using the
environment variables listed above or by passing the path directly to the
respective `load_config()` helper.

## Data product backends

The service stack now exposes ODPS data product endpoints alongside the
contract and governance APIs. Three implementations ship with the repository:

- `LocalDataProductServiceBackend` keeps definitions in memory and is ideal for
  unit tests.
- `FilesystemDataProductServiceBackend` persists each ODPS document as a JSON
  file that matches the official schema, making it a good fit for local
  sandboxes and CI environments.
- `CollibraDataProductServiceBackend` delegates persistence to Collibra through
  pluggable adapters. Pair it with
  `StubCollibraDataProductAdapter` (filesystem-backed and perfect for tests) or
  `HttpCollibraDataProductAdapter` when pointing at a live Collibra deployment.

Deployments that back onto Collibra can therefore reuse the same
`DataProductServiceClient` APIs as local runs. Swap between in-memory,
filesystem, and Collibra-backed options without touching pipeline code while
retaining draft-registration behaviour across environments.
