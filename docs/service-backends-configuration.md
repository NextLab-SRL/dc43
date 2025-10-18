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
  * `DC43_CONTRACT_STORE_TYPE` – forces the contract store implementation
    without editing the TOML file (`filesystem`, `sql`, `delta`, `collibra_stub`,
    `collibra_http`).
  * `DC43_CONTRACT_STORE_DSN` – provides a SQLAlchemy DSN when using the SQL
    backend.
  * `DC43_CONTRACT_STORE_TABLE` / `DC43_CONTRACT_STORE_SCHEMA` – override the
    table or schema used by SQL and Delta stores.
  * `DC43_BACKEND_TOKEN` – overrides the bearer token required by the HTTP API.
* Contracts app:
  * `DC43_CONTRACTS_APP_WORK_DIR` / `DC43_DEMO_WORK_DIR` – overrides the
    workspace directory.
* Additional backend specific overrides exist for the contracts UI (see
  [Contracts app configuration](#contracts-app-configuration)).

## Service backend configuration schema

The service backend configuration supports five core tables:
`contract_store`, `data_product`, `data_quality`, `governance_store`, and `auth`.

```toml
[contract_store]
type = "filesystem"
root = "./contracts"

[data_product]
type = "memory"

[data_quality]
type = "local"

[governance_store]
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
| `sql` | Persists contracts in a relational database through `SQLContractStore`. Compatible with PostgreSQL, MySQL, SQL Server, SQLite, and any SQLAlchemy-supported backend. |
| `delta` | Persists contracts in a Delta table or Unity Catalog object via `DeltaContractStore`. Requires `pyspark` and a Spark runtime. |
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

#### SQL contract store

| Key | Type | Description |
| --- | ---- | ----------- |
| `dsn` | string | **Required.** SQLAlchemy connection string (e.g. `postgresql+psycopg://user:pass@host/db`). Works with Azure SQL, Amazon RDS, Google Cloud SQL, or local engines such as SQLite. |
| `table` | string | Optional table name used for persistence. Defaults to `contracts`. |
| `schema` | string | Optional database schema or namespace that contains the contracts table. |

Install the `sql` optional dependency (`pip install dc43-service-backends[sql]`) or add `sqlalchemy` to your environment before enabling this backend. Delta-backed storage already covers Spark-native Delta tables, so use the SQL store for managed relational services on Azure, AWS, or self-hosted databases.

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

### Configuring the data-quality backend

`[data_quality]` controls how the backend evaluates contract expectations.
Supported types are:

| Type | Description |
| ---- | ----------- |
| `local` | Runs the bundled engine inside the FastAPI process. Useful for filesystem demos or when you already ship expectations with the service deployment. |
| `http` | Delegates evaluations to an external HTTP API via `RemoteDataQualityServiceBackend`. Ideal when relying on managed observability platforms or custom enforcement services. |

Common keys include:

| Key | Type | Applies to | Description |
| --- | ---- | ---------- | ----------- |
| `base_url` | string | `http` | Base URL of the remote quality service. Required when `type = "http"`. |
| `token` | string | `http` | Optional bearer token forwarded to the remote service. |
| `token_header` | string | `http` | Header used for bearer authentication (defaults to `Authorization`). |
| `token_scheme` | string | `http` | Prefix applied before the token value (defaults to `Bearer`). |
| `headers` | table | `http` | Extra static headers injected into every request. |

Environment overrides mirror other sections: `DC43_DATA_QUALITY_BACKEND_TYPE`,
`DC43_DATA_QUALITY_BACKEND_URL`, `DC43_DATA_QUALITY_BACKEND_TOKEN`,
`DC43_DATA_QUALITY_BACKEND_TOKEN_HEADER`, and
`DC43_DATA_QUALITY_BACKEND_TOKEN_SCHEME`. Use
`DC43_DATA_QUALITY_DEFAULT_ENGINE` to override the engine selected when
contracts do not specify one.

#### Configuring execution engines

Local deployments can register additional execution engines under
`[data_quality.engines]`. Each table name becomes the engine key that contracts
may reference via `metadata.quality_engine` or per-field `quality[].engine`
values. Supported engine `type` values are:

| Type | Description |
| ---- | ----------- |
| `native` / `builtin` | Wraps the contract-driven validator included with dc43. Supports `strict_types`, `allow_extra_columns`, and `expectation_severity` overrides. |
| `great_expectations` | Consumes summaries emitted by Great Expectations pipelines. Accepts `metrics_key` (defaults to the engine name) and `suite_path`/`expectations_path` for describing suites in the UI. |
| `soda` | Interprets Soda scan outcomes. Configure `metrics_key` and `checks_path` to point at Soda checks for documentation. |

Example:

```toml
[data_quality]
default_engine = "great_expectations"

[data_quality.engines.native]
type = "native"
strict_types = false

[data_quality.engines.great_expectations]
suite_path = "./expectations/orders.json"
```

When `default_engine` is set, any contract that does not request a specific
engine inherits that value. Remote data-quality services ignore the engine
configuration because execution happens outside the FastAPI process.

### Configuring the governance store

`[governance_store]` controls where validation results, dataset links, and
pipeline activity are persisted. Supported types are:

| Type | Description |
| ---- | ----------- |
| `memory` | Stores governance metadata in process memory. Useful for lightweight demos. |
| `filesystem` | Persists status snapshots, pipeline activity, and dataset links as JSON artefacts under `root`. |
| `sql` | Uses a relational database via `SQLGovernanceStore`. Requires `sqlalchemy`. |
| `delta` | Writes governance artefacts to Delta tables using Spark. Requires `pyspark`. |
| `http` | Delegates persistence to an external HTTP service implementing the governance store API. |

Common keys include `root`/`base_path` (filesystem and Delta), `dsn` and
`schema` (SQL), `status_table`/`activity_table`/`link_table` (SQL and Delta),
and `base_url`/`token`/`headers` (HTTP). Environment overrides follow the
pattern `DC43_GOVERNANCE_STORE_*`, for example
`DC43_GOVERNANCE_STORE_TYPE`, `DC43_GOVERNANCE_STORE`,
`DC43_GOVERNANCE_STORE_URL`, and `DC43_GOVERNANCE_STORE_TOKEN`.

> When the service uses Delta-backed contract or data product stores, the
> process must run in an environment that can authenticate against the target
> Unity Catalog or Delta Lake deployment. The data-quality backend—local or
> remote—can run independently; remote services only need network access to the
> tables if they compute expectations directly against the storage layer.

#### Authentication

The `auth` table lets you require clients to supply a bearer token with each
request.

| Key | Type | Description |
| --- | ---- | ----------- |
| `token` | string | Optional token value compared against the `Authorization: Bearer <token>` header. Set to an empty string to disable token authentication, which is useful for local development. |

## Contracts app configuration

The contracts UI reads three tables: `workspace`, `backend`, and `docs_chat`.

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
| `docs_chat` | `enabled` | Toggle the documentation assistant (`DC43_CONTRACTS_APP_DOCS_CHAT_ENABLED`). |
| `docs_chat` | `provider` | LLM provider identifier (currently `openai`). Override via `DC43_CONTRACTS_APP_DOCS_CHAT_PROVIDER`. |
| `docs_chat` | `model` | Chat completion model requested from the provider (`DC43_CONTRACTS_APP_DOCS_CHAT_MODEL`). |
| `docs_chat` | `embedding_provider` | Embedding backend used to build the FAISS index (defaults to `huggingface`; set to `openai` to reuse hosted embeddings). Override via `DC43_CONTRACTS_APP_DOCS_CHAT_EMBEDDING_PROVIDER`. |
| `docs_chat` | `embedding_model` | Embedding model used to build the Markdown index (`DC43_CONTRACTS_APP_DOCS_CHAT_EMBEDDING_MODEL`). Leave empty when `embedding_provider = "huggingface"` to use the bundled `sentence-transformers/all-MiniLM-L6-v2` default. |
| `docs_chat` | `api_key_env` | Environment variable that stores the provider key (`DC43_CONTRACTS_APP_DOCS_CHAT_API_KEY_ENV`). |
| `docs_chat` | `api_key` | Optional inline provider key stored directly in the configuration (keep the file outside version control). |
| `docs_chat` | `docs_path` | Optional override pointing at the directory that stores Markdown documentation (`DC43_CONTRACTS_APP_DOCS_CHAT_PATH`). |
| `docs_chat` | `index_path` | Directory used to persist the LangChain/FAISS index (`DC43_CONTRACTS_APP_DOCS_CHAT_INDEX`). |
| `docs_chat` | `code_paths` | Additional source directories to index alongside the bundled docs (`DC43_CONTRACTS_APP_DOCS_CHAT_CODE_PATHS`). |
| `docs_chat` | `reasoning_effort` | Optional reasoning hint for OpenAI `o4`/`o1` models (mirrors `DC43_CONTRACTS_APP_DOCS_CHAT_REASONING_EFFORT`). |

`api_key_env` records the *name* of the variable that contains your secret—load
the key separately (for example by exporting `OPENAI_API_KEY`, pointing the demo
at a `.env` file with `dc43-demo --env-file`, or using `direnv`). Prefer keeping
credentials outside source control? populate `docs_chat.api_key` in a private
TOML file and launch the demo with `dc43-demo --config /path/to/contracts-app.toml`.

When `docs_chat.enabled` is `true` the UI mounts a Gradio-powered assistant at
`/docs-chat/assistant` and exposes an HTML entry point under `/docs-chat`. Install
the `docs-chat` optional dependency (`pip install --no-cache-dir -e ".[demo]"`
from a source checkout, or `pip install "dc43-contracts-app[docs-chat]"` from
PyPI) and supply the configured API key variable before enabling the feature.
Avoid chaining both commands in the same environment—pip treats the editable and
wheel installs as conflicting requirements when they target the same local
package. By default the assistant indexes Markdown under `docs/` and the source
trees in `src/` and `packages/` from your dc43 checkout, ignoring paths outside
the repository even when the project sits inside a larger mono-repo. Populate
`code_paths` when you want to extend or restrict that scope. Teams experimenting
with reasoning-capable OpenAI models can
set `model = "o4-mini"` (for example) and provide a `reasoning_effort` string
(`"medium"` or `"high"`) to balance quality versus latency.

Hugging Face embeddings are enabled by default so local warm-ups avoid OpenAI's
token limits. When you prefer OpenAI-managed embeddings, set
`embedding_provider = "openai"` and choose a compatible `embedding_model`. The
docs-chat extra already includes `langchain-huggingface` and
`sentence-transformers`, so leaving `embedding_model` empty keeps the
`sentence-transformers/all-MiniLM-L6-v2` default. Run
`dc43-docs-chat-index --config /path/to/contracts-app.toml` after updating your
configuration to pre-compute the FAISS cache and reuse it across deployments.

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
