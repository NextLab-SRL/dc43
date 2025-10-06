# SQL-backed contract store

`SQLContractStore` keeps Open Data Contract Standard (ODCS) documents in a
relational database using SQLAlchemy Core. It complements the filesystem and
Delta implementations by targeting managed SQL services—Azure SQL Database,
Amazon RDS (PostgreSQL/MySQL), Google Cloud SQL, self-hosted PostgreSQL, MySQL,
SQL Server, or even SQLite for quick tests.

## When to choose it

- **Managed relational databases** – reuse existing backup, replication, and
  audit policies provided by your cloud database service.
- **Central governance warehouses** – expose contracts and metadata via SQL so
  analytics teams can join them with catalog or monitoring tables.
- **Hybrid deployments** – keep the same backend for authors running locally
  (SQLite) and production environments (Azure SQL, RDS) by swapping the DSN.

Use the Delta-backed store when you specifically need Spark-native Delta Lake
features such as time travel or lakehouse optimisations. Delta already relies
on SQL for discovery, but it requires a Spark runtime. The SQL store works with
standard SQL servers and drivers and therefore covers “regular” SQL use cases.
Both implementations share the same contract schema and serialisation helpers,
so switching between them simply changes the execution engine (Spark vs.
SQLAlchemy) without rewriting documents.

## Configuration

Enable the store through the service backend configuration:

```toml
[contract_store]
type = "sql"
dsn = "postgresql+psycopg://user:password@db.internal/dc43"
table = "contracts"           # Optional (defaults to "contracts")
schema = "governance"         # Optional
```

Key fields:

- `dsn` – SQLAlchemy connection string. Works with drivers such as `psycopg`
  (PostgreSQL), `pymysql` (MySQL/MariaDB), `pyodbc` (SQL Server), or `sqlite`. Set
  environment variable `DC43_CONTRACT_STORE_DSN` to override at runtime.
- `table` – Custom table name. Defaults to `contracts`.
- `schema` – Optional database schema/namespace when your engine supports it.

Install SQL support using the optional dependency:

```bash
pip install dc43-service-backends[sql]
```

Alternatively, add `sqlalchemy>=2.0` to your environment manually.

## Table schema

The store automatically creates the table (within the optional schema) when it
initialises. Columns:

| Column | Type | Notes |
| ------ | ---- | ----- |
| `contract_id` | `VARCHAR(255)` | Part of the primary key. |
| `version` | `VARCHAR(64)` | Part of the primary key. Stores semantic versions. |
| `name` | `VARCHAR(255)` | Display name of the contract (falls back to the identifier). |
| `description` | `TEXT` | Optional usage description extracted from the ODCS document. |
| `json` | `TEXT` | Canonical ODCS JSON payload. |
| `fingerprint` | `VARCHAR(64)` | SHA-256 fingerprint for change detection. |
| `created_at` | `TIMESTAMP WITH TIME ZONE` | Automatically populated on insert. |

Existing rows are updated when a contract with the same `contract_id` and
`version` is written again, keeping the table compatible with migrations that
replay historical documents.

## Environment-specific guidance

### Local development

1. Install the optional SQL extra: `pip install dc43-service-backends[sql]`.
2. Start a local database. For quick iteration use SQLite by pointing the DSN
   at a file (for example `sqlite:///./contracts.db`). If you already run a
   PostgreSQL or MySQL instance locally, reuse it by constructing the matching
   SQLAlchemy DSN (for example
   `postgresql+psycopg://postgres:postgres@localhost:5432/dc43`).
3. Update `dc43-service-backends.toml` in your repository or override the
   `DC43_CONTRACT_STORE_DSN` environment variable before starting the service.
4. Launch the backend (for example with `dc43 service-backends serve ...`). The
   store will automatically create the table and keep it in sync as you edit
   contracts.

### Azure deployment

1. Provision an Azure SQL Database (or Azure Database for PostgreSQL/MySQL)
   alongside your service. Note the fully qualified connection string.
2. Store credentials in Azure Key Vault or configure managed identity access
   for the compute surface (Azure Container Apps, AKS, App Service).
3. Expose the DSN to the service via `DC43_CONTRACT_STORE_DSN`. When using
   managed identity, create a DSN that leverages the appropriate authentication
   plugin (for example `sqlserver+pyodbc://@server.database.windows.net/db` with
   `Authentication=ActiveDirectoryMsi` in `odbc_connect`).
4. Deploy the service container or function with the SQL extra installed. On
   Azure Container Apps or AKS, bake `pip install dc43-service-backends[sql]`
   into the image or layer it during startup.
5. Apply firewall rules or private endpoints so the service can reach the SQL
   database, then restart the workload. The contract table will appear in the
   configured schema during the first run.

The Terraform stacks under `deploy/terraform/azure-service-backend` and
`deploy/terraform/aws-service-backend` expose `contract_store_mode = "sql"`
switches. When enabled, the modules provision the correct environment variables
for `DC43_CONTRACT_STORE_TYPE` and `DC43_CONTRACT_STORE_DSN` so the service uses
the relational backend without mounting shared storage.

### AWS deployment

1. Create an Amazon RDS or Aurora instance that matches your preferred engine
   (PostgreSQL/MySQL). Capture the host, port, and database name.
2. Manage credentials with AWS Secrets Manager or IAM database authentication
   tokens. For IAM tokens, generate them during startup and inject them into the
   DSN (for example
   `postgresql+psycopg://db_user:<token>@instance.cluster-XYZ.us-east-1.rds.amazonaws.com:5432/dc43`).
3. Configure the hosting environment (ECS, EKS, Lambda, or EC2) to export
   `DC43_CONTRACT_STORE_DSN`. Rotate the secret by updating the environment
   variable or task definition; the store will pick up the change on restart.
4. Ensure the workload can reach the database through VPC security groups or
   PrivateLink endpoints.
5. Deploy the service container with the SQL extra installed. The store will
   create or migrate the contracts table automatically during bootstrap.

Because SQLAlchemy abstracts vendors, the same `SQLContractStore` works across
providers. Pair it with your preferred high availability configuration, backup
policies, and credential rotation workflows. For Google Cloud SQL deployments,
reuse the same pattern by running the Cloud SQL Auth proxy or using the Python
connector dialects in the DSN.
