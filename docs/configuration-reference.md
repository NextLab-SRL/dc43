# dc43 configuration reference

This guide lists every configuration surface exposed by the dc43 project and the
values captured by the setup wizard. Use it as a single source of truth when
building automation around the generated `setup_state.json`, exporting bundles,
or wiring TOML configuration into your own infrastructure.

Need sample credentials while exploring the wizard? Step 2 now includes a
**Generate sample configuration** button that reads from
`dc43_contracts_app/static/setup-wizard-template.json`. The template contains
example values for every module and option so you can populate the current form
with realistic placeholders before customising the output for your environment.

## Configuration surfaces at a glance

| Surface | Description | How to configure |
| ------- | ----------- | ---------------- |
| Setup wizard (`/setup`) | Interactive UI that writes `setup_state.json`, exports bundles, and powers the architecture diagram. | Complete the three wizard steps or automate them via the Playwright scenarios under `packages/dc43-contracts-app/tests/playwright`. |
| Contracts app TOML (`dc43_contracts_app.config`) | Controls the FastAPI UI, workspace paths, backend connection mode, and docs chat. | Place overrides in a TOML file and set `DC43_CONTRACTS_APP_CONFIG`, or rely on environment variables documented below. |
| Service backends TOML (`dc43_service_backends.config`) | Configures contract/data-product stores, governance persistence, auth, and optional Unity Catalog hooks for the backend APIs. | Provide a TOML file referenced by `DC43_SERVICE_BACKENDS_CONFIG`, then layer environment overrides on top. |
| Environment variables | Fine-grained overrides read by both configuration loaders. | Export the variables described in this guide before launching the apps or running the demo CLI. |

## Setup wizard modules

Every module listed below appears in the setup wizard. The module key matches the
structure stored under `configuration` in `setup_state.json`. Exported bundles
also include per-module TOML files under `dc43-setup/config/modules/` that mirror
the raw field values captured during setup.

> **Note:** Standalone deployments no longer derive filesystem defaults for these
> modules. The demo application registers optional workspace hints so its sample
> flows still pre-populate paths; when running the contracts app on its own,
> enter any filesystem locations required by the selected backends manually.

### Contracts storage backend (`contracts_backend`)

#### Local filesystem (`filesystem`)
*Required*
- `work_dir` – Root directory bound to `DC43_CONTRACTS_APP_WORK_DIR` when the UI launches.
- `contracts_dir` – Workspace-relative folder where contract files are created.

#### Collibra governance backend (`collibra`)
*Required*
- `base_url` – Fully qualified Collibra site URL.
- `client_id` – OAuth client identifier for the automation user.
- `client_secret` – OAuth client secret stored in your secrets manager.
- `domain_id` – Collibra domain that will own the contract assets.

#### SQL database (`sql`)
*Required*
- `connection_uri` – SQLAlchemy compatible DSN (for example `postgresql+psycopg://user:pass@host:5432/contracts`).
- `schema` – Database schema where contract tables live.

*Optional*
- `ssl_mode` – Driver-specific SSL requirement flag such as `require` or `verify-full`.

#### Delta Lake (SQL-on-lake) (`delta_lake`)
*Required*
- `schema` – Schema/database that groups the contract Delta tables.

*Optional*
- `storage_path` – External Delta location (S3/ABFS/etc.) when not using Unity-managed tables.
- `table_name` – Fully qualified Unity Catalog table name when relying on managed tables.
- `workspace_url` – Databricks workspace URL that hosts the Delta catalog.
- `workspace_profile` – Profile from `databricks.cfg` for profile-based auth.
- `workspace_token` – Personal access token exposed as `DATABRICKS_TOKEN` for Spark sessions.
- `catalog` – Unity Catalog or metastore catalog name backing the tables.

### Data products backend (`products_backend`)

#### Local filesystem (`filesystem`)
*Required*
- `products_dir` – Folder storing published product descriptors (JSON/YAML).

#### Collibra domain (`collibra`)
*Required*
- `base_url` – Collibra instance hosting product assets.
- `client_id` – OAuth client for the product synchronisation job.
- `client_secret` – Secret required for API authentication.
- `domain_id` – Collibra domain identifier containing product assets.

#### SQL database (`sql`)
*Required*
- `connection_uri` – SQLAlchemy DSN for the product metadata database.
- `schema` – Schema where product tables are located.

#### Delta Lake (`delta_lake`)
*Required*
- `catalog` – Unity Catalog/metastore catalog for product Delta tables.
- `schema` – Schema/database that holds the product Delta tables.

*Optional*
- `storage_path` – External Delta path when not using managed tables.
- `table_name` – Fully qualified Unity Catalog table name for managed storage.
- `workspace_url` – Databricks workspace URL backing the tables.
- `workspace_profile` – Databricks CLI profile name.
- `workspace_token` – Personal access token for Spark or REST clients.

### Data quality service (`data_quality`)

#### Embedded local engine (`embedded_engine`)
*Required*
- `expectations_path` – Directory containing `.yml`/`.json` expectation suites.

*Optional*
- `results_path` – Optional folder where validation outputs are written.

#### Remote data-quality API (`remote_http`)
*Required*
- `base_url` – HTTPS endpoint for the remote data-quality service.

*Optional*
- `api_token` – Bearer or PAT credential.
- `token_header` – HTTP header used to transmit the token (defaults to `Authorization`).
- `token_scheme` – Scheme/prefix prepended to the token value (for example `Bearer`).
- `default_engine` – Default engine identifier (for example `soda`).
- `extra_headers` – Additional request headers (newline- or comma-separated key/value pairs).

### Validation results storage (`governance_store`)

#### In-memory cache (`embedded_memory`)
No additional fields are captured.

#### Filesystem archive (`filesystem`)
*Required*
- `storage_path` – Local or network path used to persist governance artefacts.

#### SQL database (`sql`)
*Required*
- `connection_uri` – SQLAlchemy DSN pointing at the governance database.

*Optional*
- `schema` – Database schema owning the governance tables.
- `status_table` – Table name for run statuses.
- `activity_table` – Table that records governance actions.
- `link_table` – Table linking datasets/contracts to governance runs.

#### Delta Lake (`delta_lake`)
*Optional*
- `storage_path` – External Delta path backing the governance tables.
- `status_table` – Unity Catalog table name for run statuses.
- `activity_table` – Unity Catalog table for governance events.
- `link_table` – Unity Catalog table linking governance artefacts.
- `workspace_url` – Databricks workspace URL for the Delta catalog.
- `workspace_profile` – Databricks CLI profile name.
- `workspace_token` – Personal access token for Databricks.

#### Remote governance API (`remote_http`)
*Required*
- `base_url` – HTTPS endpoint for the governance storage API.

*Optional*
- `api_token` – Access token supplied to the API.
- `token_header` – Header carrying the token (defaults to `Authorization`).
- `token_scheme` – Prefix applied to the token (defaults to `Bearer`).
- `timeout` – Request timeout in seconds.
- `extra_headers` – Additional headers passed to the API.

### Pipeline integration (`pipeline_integration`)

Set the governance publication mode with the `publication_mode` field, the
`DC43_GOVERNANCE_PUBLICATION_MODE` environment variable, or Spark configuration
keys (`dc43.governance.publicationMode`, `dc43.governance.publication_mode`, or
`governance.publication.mode`). Supported values are `legacy`,
`open_data_lineage`, and `open_telemetry`; the legacy behaviour remains the
default when no hint is provided.

#### Apache Spark (`spark`)
*Optional*
- `runtime` – Free-form runtime hint (for example `databricks job`).
- `workspace_url` – Databricks workspace URL used by the pipeline.
- `workspace_profile` – Databricks CLI profile tied to the runtime.
- `cluster_reference` – Cluster identifier, job name, or pool reference consumed by the bootstrap script.
- `publication_mode` – Overrides the governance publication mode for Spark
  helpers when present. Accepts `legacy`, `open_data_lineage`, or
  `open_telemetry`.

#### Databricks Delta Live Tables (`dlt`)
*Required*
- `workspace_url` – Databricks workspace URL hosting the DLT pipeline.
- `pipeline_name` – Name of the Delta Live Tables pipeline.

*Optional*
- `workspace_profile` – Databricks CLI profile used for authentication.
- `notebook_path` – Workspace notebook path for pipeline code.
- `target_schema` – Target schema/database for published tables.
- `publication_mode` – Optional override mirroring the Spark integration and
  accepting the same `legacy`, `open_data_lineage`, or `open_telemetry`
  values.

### Governance interface (`governance_service`)

#### Embedded web service (`embedded_monolith`)
No additional fields are captured.

#### Direct Python orchestration (`direct_runtime`)
No additional fields are captured.

#### Remote governance API (`remote_api`)
*Required*
- `base_url` – HTTPS endpoint of the standalone governance service.

*Optional*
- `api_token` – Authentication token for the remote service.

### Governance service deployment (`governance_deployment`)

#### Local Python process (`local_python`)
*Optional*
- `command` – Launch command advertised to operators (for example `uvicorn dc43_contracts_app.server:app --reload --port 8000`).

#### Local Docker or Compose (`local_docker`)
No additional fields are captured.

#### AWS (Terraform) (`aws_terraform`)
*Required*
- `aws_region` – AWS region hosting the deployment.
- `cluster_name` – ECS cluster name serving the governance tasks.
- `ecr_image_uri` – Container image URI pushed to Amazon ECR.
- `private_subnet_ids` – Comma-separated private subnet IDs for the ECS service.
- `load_balancer_subnet_ids` – Comma-separated subnet IDs for the public load balancer.
- `service_security_group_id` – Security group applied to the ECS tasks.
- `load_balancer_security_group_id` – Security group applied to the Application Load Balancer.
- `certificate_arn` – ACM certificate ARN used for HTTPS ingress.
- `vpc_id` – VPC that hosts the deployment.

*Optional*
- `backend_token` – Token injected into the deployed service for API auth.
- `contract_store_mode` – Storage backend mode advertised to Terraform.
- `contract_filesystem` – Filesystem path when using the filesystem backend.
- `contract_storage_path` – External object storage path for Delta backends.
- `contract_store_dsn` – SQL DSN when persisting contracts in a database.
- `contract_store_dsn_secret_arn` – ARN of a Secrets Manager entry containing the SQL DSN.
- `contract_store_table` – Contract table name in SQL/Delta backends.
- `contract_store_schema` – Schema containing the contract table.
- `task_cpu` – Requested CPU for the ECS task definition.
- `task_memory` – Requested memory for the ECS task definition.
- `desired_count` – Initial ECS service replica count.
- `container_port` – Container port exposed to the load balancer.
- `health_check_path` – HTTP path used for load balancer health checks.
- `health_check_interval` – Seconds between load balancer health probes.
- `health_check_timeout` – Timeout for health probes.
- `health_check_healthy_threshold` – Successful probe count before marking healthy.
- `health_check_unhealthy_threshold` – Failed probe count before marking unhealthy.
- `log_retention_days` – CloudWatch log retention period in days.

#### Azure (Terraform) (`azure_terraform`)
*Required*
- `subscription_id` – Azure subscription identifier hosting the deployment.
- `resource_group_name` – Resource group name for the Container App.
- `location` – Azure region.
- `container_registry` – Azure Container Registry host serving the image.
- `container_registry_username` – Registry username with pull permissions.
- `container_registry_password` – Registry password or access key.
- `image_tag` – Fully qualified container image reference (`repository:tag`).

*Optional*
- `backend_token` – Token injected into the deployed service for API auth.
- `contract_store_mode` – Storage backend mode advertised to Terraform.
- `contract_storage` – Storage account name used by the filesystem backend.
- `contract_share_name` – Azure Files share for contract persistence.
- `contract_share_quota_gb` – Share quota when provisioning Azure Files.
- `contract_store_dsn` – SQL DSN when persisting contracts in a database.
- `contract_store_table` – Contract table name in SQL/Delta backends.
- `contract_store_schema` – Schema containing the contract table.
- `container_app_environment_name` – Container Apps environment name.
- `container_app_name` – Container App resource name.
- `ingress_port` – Public port exposed by the Container App.
- `min_replicas` – Minimum replica count for autoscaling.
- `max_replicas` – Maximum replica count for autoscaling.
- `container_cpu` – vCPU allocation per replica.
- `container_memory` – Memory allocation per replica (GiB).
- `tags` – Comma-separated resource tags (`key=value`).

#### Not required (direct runtime) (`not_required`)
No additional fields are captured (selection is only available when the governance service runs in-process).

### Governance hooks (`governance_extensions`)

#### None (`none`)
No additional fields are captured.

#### Unity Catalog synchronisation (`unity_catalog`)
*Required*
- `workspace_url` – Databricks workspace URL hosting the Unity Catalog.
- `catalog` – Unity Catalog name.
- `schema` – Schema within the catalog.
- `token` – Access token used by the hook.

*Optional*
- `dataset_prefix` – Prefix applied to published dataset identifiers (defaults to `table:`).
- `workspace_profile` – Databricks CLI profile used for authentication.
- `static_properties` – Optional newline-separated `key=value` pairs forwarded to Unity Catalog.

#### Custom Python module (`custom_module`)
*Required*
- `module_path` – Import path exposing the governance hook entry points.

*Optional*
- `config_path` – Path to a YAML/JSON configuration file consumed by the hook.

### User interface (`user_interface`)

#### Bundled web application (`local_web`)
No additional fields are captured (all configuration relies on environment variables from other modules).

#### Hosted portal (`remote_portal`)
*Required*
- `portal_url` – Base URL that operators use to access the hosted UI.

### Documentation assistant (`docs_assistant`)

#### Disabled (`disabled`)
No configuration values are persisted.

#### Gradio assistant (OpenAI) (`openai_embedded`)
*Required*
- `provider` – Provider identifier (defaults to `openai`).
- `model` – Chat model name (defaults to `gpt-4o-mini`).
- `embedding_model` – Embedding model identifier (defaults to `text-embedding-3-small`).
- `api_key_env` – Environment variable that will hold the provider API key (`OPENAI_API_KEY` by default).

*Optional*
- `docs_path` – Override for the documentation directory indexed by the assistant.
- `index_path` – Override for the persisted vector index location.

### User interface deployment (`ui_deployment`)

#### Local Python process (`local_python`)
*Optional*
- `command` – Launch command shared with operators (for example `uvicorn dc43_contracts_app.server:app --reload --port 8000`).

#### Local Docker or Compose (`local_docker`)
No additional fields are captured.

#### No dedicated hosting (`skip_hosting`)
No configuration values are persisted.

#### AWS (Terraform scaffold) (`aws_terraform`)
*Required*
- `aws_region` – AWS region hosting the UI.
- `cluster_name` – ECS cluster used to run the UI tasks.
- `ecr_image_uri` – ECR image reference for the UI container.
- `private_subnet_ids` – Comma-separated private subnet IDs for the ECS service.
- `load_balancer_subnet_ids` – Comma-separated subnet IDs for the public load balancer.
- `service_security_group_id` – Security group attached to the ECS tasks.
- `load_balancer_security_group_id` – Security group attached to the load balancer.
- `certificate_arn` – ACM certificate ARN for HTTPS ingress.
- `vpc_id` – VPC hosting the deployment.

*Optional*
- `service_name` – ECS service name.
- `container_port` – Container port exposed to the load balancer.
- `desired_count` – Desired ECS task replica count.
- `task_cpu` – CPU allocation for the ECS task definition.
- `task_memory` – Memory allocation for the ECS task definition.
- `health_check_path` – HTTP path used for load balancer health checks.
- `health_check_interval` – Seconds between health probes.
- `health_check_timeout` – Probe timeout duration.
- `health_check_healthy_threshold` – Successful probe count before marking healthy.
- `health_check_unhealthy_threshold` – Failed probe count before marking unhealthy.
- `log_retention_days` – CloudWatch log retention period.

#### Azure (Terraform scaffold) (`azure_terraform`)
*Required*
- `subscription_id` – Azure subscription identifier hosting the UI deployment.
- `resource_group_name` – Resource group name.
- `location` – Azure region.
- `container_registry` – Azure Container Registry host serving the UI image.
- `container_registry_username` – Registry username with pull permissions.
- `container_registry_password` – Registry password or access key.
- `image_tag` – Container image reference (`repository:tag`).

*Optional*
- `container_app_environment_name` – Container Apps environment name.
- `container_app_name` – Container App resource name.
- `ingress_port` – Public port exposed by the Container App.
- `min_replicas` – Minimum replica count.
- `max_replicas` – Maximum replica count.
- `container_cpu` – vCPU allocation per replica.
- `container_memory` – Memory allocation per replica (GiB).
- `tags` – Comma-separated resource tags applied to Azure resources.

### Demo automation (`demo_automation`)

#### Do not launch the demo (`skip_demo`)
No configuration values are persisted.

#### Run demo locally (Python) (`local_python`)
No additional fields are captured.

#### Run demo locally (Docker) (`local_docker`)
No additional fields are captured.

### Authentication & access (`authentication`)

#### No authentication (`none`)
No configuration values are persisted.

#### HTTP basic auth (`basic`)
*Required*
- `username` – Login presented to the UI.
- `password` – Password stored alongside the deployment secrets.

#### OAuth / OIDC (`oauth_oidc`)
*Required*
- `issuer_url` – OIDC discovery endpoint for the identity provider.
- `client_id` – Registered application (client) identifier.
- `client_secret` – Client secret or credential used for token exchange.
- `redirect_uri` – Callback URL used after authentication completes.

## Contracts app TOML (`dc43_contracts_app.config`)

`dc43_contracts_app.config.load_config()` loads configuration from the first
existing path in `[explicit_path, $DC43_CONTRACTS_APP_CONFIG, default.toml]`. When
an explicit path is provided the loader skips most environment overrides so
TOML files remain authoritative.

### `[workspace]`
- `root` (`Path | None`) – Optional base directory used for filesystem hints
  and backwards-compatible demos. The contracts UI no longer creates or
  manages this path automatically. Override with `DC43_CONTRACTS_APP_WORK_DIR`
  (or `DC43_DEMO_WORK_DIR`).

> **Note:** Use `DC43_CONTRACTS_APP_STATE_DIR` to relocate the setup wizard
> persistence and docs-chat cache directories when running the standalone UI.
> When unset the app writes to `~/.dc43-contracts-app`.

### `[backend]`
- `mode` (`embedded` | `remote`) – Determines whether the UI starts the backend in-process. Override with `DC43_CONTRACTS_APP_BACKEND_MODE`.
- `base_url` (`str | None`) – Remote backend URL when `mode="remote"`. Override with `DC43_CONTRACTS_APP_BACKEND_URL` (or `DC43_DEMO_BACKEND_URL`).

#### `[backend.process]`
- `host` – Host used when launching the embedded backend (defaults to `127.0.0.1`). Override with `DC43_CONTRACTS_APP_BACKEND_HOST` / `DC43_DEMO_BACKEND_HOST`.
- `port` – Port used for the embedded backend (defaults to `8001`). Override with `DC43_CONTRACTS_APP_BACKEND_PORT` / `DC43_DEMO_BACKEND_PORT`.
- `log_level` – Optional Uvicorn log level. Override with `DC43_CONTRACTS_APP_BACKEND_LOG` / `DC43_DEMO_BACKEND_LOG`.

### `[docs_chat]`
- `enabled` – Toggle for the documentation assistant (`false` by default). Override with `DC43_CONTRACTS_APP_DOCS_CHAT_ENABLED`.
- `provider` – Chat provider identifier (`openai`). Override with `DC43_CONTRACTS_APP_DOCS_CHAT_PROVIDER`.
- `model` – Chat model (`gpt-4o-mini`). Override with `DC43_CONTRACTS_APP_DOCS_CHAT_MODEL`.
- `embedding_provider` – Embedding backend (`huggingface`). Override with `DC43_CONTRACTS_APP_DOCS_CHAT_EMBEDDING_PROVIDER`.
- `embedding_model` – Embedding model identifier. Override with `DC43_CONTRACTS_APP_DOCS_CHAT_EMBEDDING_MODEL`.
- `api_key_env` – Environment variable that supplies the API key. Override with `DC43_CONTRACTS_APP_DOCS_CHAT_API_KEY_ENV`.
- `api_key` – Inline secret used instead of an environment variable. Override with `DC43_CONTRACTS_APP_DOCS_CHAT_API_KEY`.
- `docs_path` – Optional documentation directory override. Override with `DC43_CONTRACTS_APP_DOCS_CHAT_PATH`.
- `index_path` – Optional index directory override. When omitted the UI stores
  cached FAISS files under `~/.dc43/docs_chat/index`. Override with
  `DC43_CONTRACTS_APP_DOCS_CHAT_INDEX`.
- `code_paths` – Tuple of extra directories to index. Override with `DC43_CONTRACTS_APP_DOCS_CHAT_CODE_PATHS` (comma/semicolon/`os.pathsep` separated).
- `reasoning_effort` – Optional reasoning depth hint for OpenAI reasoning models. Override with `DC43_CONTRACTS_APP_DOCS_CHAT_REASONING_EFFORT`.

`dc43_contracts_app.config.config_to_mapping()` exposes the same structure as a
serialisable dictionary, and `dumps()`/`dump()` emit TOML text suitable for bundling
into exported archives.

## Service backends TOML (`dc43_service_backends.config`)

`dc43_service_backends.config.load_config()` checks `[explicit_path,
$DC43_SERVICE_BACKENDS_CONFIG, default.toml]` and applies environment overrides
for targeted keys.

### `[contract_store]`
- `type` – Backend type (`filesystem`, `sql`, `delta`, `collibra_stub`, `collibra_http`). Override with `DC43_CONTRACT_STORE_TYPE`.
- `root` – Filesystem root for filesystem/Collibra stub backends. Override with `DC43_CONTRACT_STORE`.
- `base_path` – External Delta path when not using managed tables. Override with `DC43_CONTRACT_STORE_BASE_PATH`.
- `table` – Table name for Delta/SQL backends. Override with `DC43_CONTRACT_STORE_TABLE`.
- `dsn` – SQLAlchemy DSN for the SQL backend. Override with `DC43_CONTRACT_STORE_DSN`.
- `schema` – Database schema for SQL/Delta backends. Override with `DC43_CONTRACT_STORE_SCHEMA`.
- `base_url` – Collibra HTTP adapter base URL.
- `token` – Bearer token for the Collibra HTTP adapter. Override with `DC43_BACKEND_TOKEN` when the token should be shared.
- `timeout` – Request timeout (seconds) for HTTP adapters.
- `contracts_endpoint_template` – Override for Collibra HTTP endpoint templates.
- `default_status` / `status_filter` – Workflow status defaults for Collibra adapters.
- `catalog` – Mapping of contract IDs to `(data_product, port)` tuples when emulating Collibra catalogues.

### `[data_product_store]`
- `type` – Backend type (`memory`, `filesystem`, `delta`, `collibra_stub`, `collibra_http`).
- `root` / `base_path` / `table` / `dsn` / `schema` – Same semantics as the contract store.
- `base_url` – Collibra HTTP adapter base URL.
- `catalog` – Collibra catalog identifier for product assets.

Environment overrides:
- `DC43_DATA_PRODUCT_STORE` – Filesystem root.
- `DC43_DATA_PRODUCT_TABLE` – Delta/SQL table name.

### `[data_quality]`
- `type` – Backend type (`local`, `http`, etc.). Override with `DC43_DATA_QUALITY_BACKEND_TYPE`.
- `base_url` – Remote data-quality API endpoint. Override with `DC43_DATA_QUALITY_BACKEND_URL`.
- `token` – Access token. Override with `DC43_DATA_QUALITY_BACKEND_TOKEN`.
- `token_header` / `token_scheme` – HTTP auth overrides. Override with `DC43_DATA_QUALITY_BACKEND_TOKEN_HEADER` / `_TOKEN_SCHEME`.
- `headers` – Additional request headers parsed from TOML tables.
- `default_engine` – Default engine identifier. Override with `DC43_DATA_QUALITY_DEFAULT_ENGINE`.
- `engines` – Engine-specific configuration payload.

### `[auth]`
- `token` – Bearer token required by the backend APIs. Override with `DC43_BACKEND_TOKEN`.

### `[unity_catalog]`
- `enabled` – Toggle for Unity Catalog synchronisation. Override with `DC43_UNITY_CATALOG_ENABLED`.
- `dataset_prefix` – Prefix applied to dataset identifiers. Override with `DC43_UNITY_CATALOG_PREFIX`.
- `workspace_profile` – Databricks CLI profile name. Override with `DATABRICKS_CONFIG_PROFILE`.
- `workspace_url` – Databricks workspace URL. Override with `DATABRICKS_HOST`. Existing configurations that still use
  `workspace_host` remain supported for backwards compatibility.
- `workspace_token` – Databricks token. Override with `DATABRICKS_TOKEN` or `DC43_DATABRICKS_TOKEN`.
- `static_properties` – Additional metadata pushed to Unity Catalog.

### `[governance]`
- `dataset_contract_link_builders` – Tuple of import paths used to build dataset→contract links. Override with `DC43_GOVERNANCE_LINK_BUILDERS` (comma-separated).

### `[governance_store]`
- `type` – Backend type (`memory`, `filesystem`, `sql`, `delta`, `http`). Override with `DC43_GOVERNANCE_STORE_TYPE`.
- `root` / `base_path` – Filesystem or object storage roots. Override with `DC43_GOVERNANCE_STORE` / `DC43_GOVERNANCE_STORE_BASE_PATH`.
- `table` / `status_table` / `activity_table` / `link_table` – Table names for SQL/Delta backends. Override with
  `DC43_GOVERNANCE_STORE_TABLE`, `DC43_GOVERNANCE_STATUS_TABLE`, `DC43_GOVERNANCE_ACTIVITY_TABLE`, `DC43_GOVERNANCE_LINK_TABLE`.
- `dsn` – SQL DSN. Override with `DC43_GOVERNANCE_STORE_DSN`.
- `schema` – Database schema. Override with `DC43_GOVERNANCE_STORE_SCHEMA`.
- `base_url` – Remote governance API endpoint. Override with `DC43_GOVERNANCE_STORE_URL`.
- `token` – Token used by the remote governance API. Override with `DC43_GOVERNANCE_STORE_TOKEN`.
- `token_header` / `token_scheme` – HTTP auth overrides. Override with
  `DC43_GOVERNANCE_STORE_TOKEN_HEADER` / `DC43_GOVERNANCE_STORE_TOKEN_SCHEME`.
- `timeout` – Request timeout in seconds. Override with `DC43_GOVERNANCE_STORE_TIMEOUT`.
- `headers` – Additional request headers parsed from TOML tables.

### Environment override precedence

When both TOML and environment values exist, the loaders prioritise environment
variables (unless an explicit config path was supplied, in which case the TOML
values remain authoritative). Use this behaviour to parameterise deployments
without mutating the generated configuration bundles.
