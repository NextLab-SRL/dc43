# Databricks teams: integrate dc43 with Unity Catalog

This walkthrough shows how to run dc43 in a Databricks workspace that already
uses Unity Catalog and Delta Lake. You will install the governance services in
an all-purpose cluster, register a simple contract and data product, and tag the
managed Delta tables so catalog stewards can see which assets are governed by
dc43.

> The goal is to validate the integration end-to-end. The steps below reuse the
> production components (contract store, data product service, Spark helpers)
> but keep everything in a single workspace so you can iterate quickly before
> exposing the services more broadly.

## 1. Workspace prerequisites

Before you start, make sure that:

- Unity Catalog is enabled for the workspace and you have permission to create a
  catalog and schema.
- Your cluster (all-purpose or job) runs the Databricks Runtime for Machine
  Learning 13.3 LTS or newer, which ships with Python 3.10 and Spark 3.4+.
- You created a [secret scope](https://docs.databricks.com/security/secrets/index.html)
  that stores the token used to call the dc43 governance APIs (only required
  when you deploy the backend behind authentication).

Run the following SQL once to create a catalog and schema that will hold the
Delta tables produced by the demo pipeline:

```sql
CREATE CATALOG IF NOT EXISTS governed
  MANAGED LOCATION 'dbfs:/mnt/dc43-demo/governed';
CREATE SCHEMA IF NOT EXISTS governed.analytics;
USE CATALOG governed;
USE SCHEMA analytics;
```

## 2. Install dc43 libraries on the cluster

Install the Spark integration helpers and the HTTP clients from a notebook cell
attached to the target cluster. `%pip` ensures the wheels are stored in the
cluster’s DBFS environment:

```python
%pip install "dc43-service-clients[http]" "dc43-integrations[spark]"
```

Restart the Python process after the install completes so the new packages are
visible to the runtime (Databricks prompts you to do this automatically).

If you prefer cluster-scoped libraries, add the same packages as PyPI
installations in the cluster configuration UI.

## 3. Configure service backends

The Spark helpers connect to the governance services through a TOML
configuration file. Store it in DBFS so every notebook can reference the same
settings:

```python
config = """
[contract_store]
type = "delta"
table = "governed.meta.contracts"

[data_product]
type = "delta"
table = "governed.meta.data_products"
"""

dbutils.fs.mkdirs("dbfs:/mnt/dc43-demo/config")
dbutils.fs.put("dbfs:/mnt/dc43-demo/config/dc43-service-backends.toml", config, True)
```

Point the helper loader at this configuration when you create a contract or
register a data product. The bootstrap helpers provision the Unity-backed
stores automatically and fall back to filesystem or remote deployments if you
change the `type` fields (for example, `filesystem` for a DBFS prototype or
`collibra_http` / an Azure-hosted backend that persists metadata in Postgres).

```python
from dc43_service_backends.bootstrap import build_backends
from dc43_service_backends.config import load_config

config = load_config("dbfs:/mnt/dc43-demo/config/dc43-service-backends.toml")
suite = build_backends(config)
contract_backend, data_product_backend = suite
dq_backend = suite.data_quality
```

When you prefer to materialise the catalogues in Unity tables, set the `table`
fields as shown above. Switching to `base_path` keeps the previous behaviour of
writing Delta files under `dbfs:/mnt/...`, which is useful for quick proofs of
concept. Remote service deployments remain viable as well: keep the
configuration pointing at your existing governance services (for example,
Postgres- or Azure-backed APIs) and the Unity Catalog hooks continue to apply
tags while the contract or product payloads live in those external stores.

## 4. Create a demo contract and data product

The demo contract describes a small `orders` table. Use the Open Data Contract
Standard helpers that ship with dc43 to build the document and push it to the
Delta-backed contract store:

```python
from open_data_contract_standard.model import (
    OpenDataContractStandard, SchemaObject, SchemaProperty, Description
)

orders_contract = OpenDataContractStandard(
    version="0.1.0",
    kind="DataContract",
    apiVersion="3.0.2",
    id="sales.orders",
    name="Orders",
    description=Description(usage="Orders facts"),
    schema_=[
        SchemaObject(
            name="orders",
            properties=[
                SchemaProperty(name="order_id", physicalType="bigint", required=True, unique=True),
                SchemaProperty(name="customer_id", physicalType="bigint", required=True),
                SchemaProperty(name="order_ts", physicalType="timestamp", required=True),
                SchemaProperty(name="amount", physicalType="double", required=True),
                SchemaProperty(
                    name="currency",
                    physicalType="string",
                    required=True,
                    logicalTypeOptions={"enum": ["EUR", "USD"]},
                ),
            ],
        )
    ],
)

contract_backend.put(orders_contract)
```

Next, publish a minimal data product with one output port that references the
contract you just stored:

```python
data_product_backend.register_output_port(
    data_product_id="dp.analytics.orders",
    port_name="primary",
    contract=orders_contract,
)
```

The backend persists both artefacts in Delta tables under the base paths you set
in the configuration file. You can inspect them directly with `spark.read.format("delta")`.
`suite.data_quality` exposes the data-quality delegate configured in the TOML
file—useful when you offload expectation evaluation to remote observability
services.

## 5. Generate a governed Delta table

Create a small synthetic dataset that adheres to the contract and write it to a
Unity Catalog table via the Spark IO helper. The helper enforces the contract
before the data hits storage and registers the output port through the
governance service, so you no longer need to call the data product client
directly. The contracts application now correlates the recorded dataset IDs and
contract bindings from this governance activity with the catalogued product
ports, so the product detail pages surface run counts even when historical
pipeline events predate the dedicated product metadata fields.

```python
from pyspark.sql import functions as F
from dc43_integrations.spark.io import (
    ContractVersionLocator,
    GovernanceSparkWriteRequest,
    write_with_governance,
)

orders_df = spark.createDataFrame(
    [
        (1, 101, "2024-01-01T10:00:00Z", 125.50, "EUR"),
        (2, 102, "2024-01-02T11:30:00Z", 75.00, "USD"),
        (3, 103, "2024-01-03T09:15:00Z", 220.00, "EUR"),
    ],
    schema="order_id long, customer_id long, order_ts string, amount double, currency string",
).withColumn("order_ts", F.to_timestamp("order_ts"))

validation, _ = write_with_governance(
    df=orders_df,
    request=GovernanceSparkWriteRequest(
        context={
            "contract": {
                "contract_id": "sales.orders",
                "version_selector": ">=0.1.0",
            },
            "output_binding": {
                "data_product": "dp.analytics.orders",
                "port_name": "primary",
            },
        },
        dataset_locator=ContractVersionLocator(dataset_version="latest"),
        path="dbfs:/mnt/dc43-demo/delta/orders",
        mode="overwrite",
    ),
    governance_service=suite.governance,
    enforce=True,
    auto_cast=True,
    return_status=True,
)

print("Validation status:", validation.status)
```

Finally, create the Unity Catalog table that surfaces the managed Delta files:

```sql
CREATE TABLE IF NOT EXISTS governed.analytics.orders
USING DELTA
LOCATION 'dbfs:/mnt/dc43-demo/delta/orders';
```

## 6. Automate Unity Catalog metadata updates

Most teams want catalog metadata to highlight whether a table is governed by a
contract or belongs to a specific data product. Instead of sprinkling tagging
statements across individual notebooks, enable the Unity Catalog bridge in the
governance backend and let the service handle both table properties and Unity
tags as part of the link workflow.

Install the Databricks SQL connector alongside the backend so the Unity Catalog
bridge can run `ALTER TABLE … SET/UNSET TBLPROPERTIES` statements through a SQL
warehouse:

```bash
pip install "dc43-service-backends[http]" databricks-sqlalchemy
```

The token or service principal embedded in the Databricks SQLAlchemy DSN must
have the following Unity Catalog privileges on the governed catalog and schema:

- `USE CATALOG`
- `USE SCHEMA`
- `ALTER` (or `OWN`) on each table that the backend will update

Grant those permissions to a dedicated service principal and create a
Databricks personal access token for it. Store the token alongside the dc43
configuration (for example in a Databricks secret scope) and point
`DC43_UNITY_CATALOG_SQL_DSN` at the rendered DSN when deploying the FastAPI app.
You can optionally export `DC43_UNITY_CATALOG_TAGS_ENABLED=1` (and, if you
prefer a different warehouse for tags, `DC43_UNITY_CATALOG_TAGS_SQL_DSN`) to
control the Unity tag helper independently of the property updater.

With the permissions in place, add Unity Catalog settings to the service backend
configuration so the web application can talk to the Databricks SQL warehouse.
The bridge is wired as a governance hook, which means you can keep the REST
interfaces completely technology agnostic and still compose multiple
implementations if needed:

```toml
[unity_catalog]
enabled = true
dataset_prefix = "table:"
sql_dsn = "databricks://token:${DATABRICKS_TOKEN}@adb-<workspace>.azuredatabricks.net?http_path=/sql/1.0/warehouses/<id>"
tags_enabled = true
# tags_sql_dsn defaults to sql_dsn when omitted

[unity_catalog.static_properties]
dc43.catalog_synced = "true"

[unity_catalog.static_tags]
owner = "governance"
environment = "sandbox"

[governance]
dataset_contract_link_builders = [
  "dc43_service_backends.governance.unity_catalog:build_link_hooks",
]
```

The DSN may omit the `catalog` or `schema` query parameters—the backend issues
fully-qualified `ALTER TABLE` statements based on the dataset identifiers it
receives. Legacy `workspace_*` keys remain accepted in the configuration for
backwards compatibility but are ignored by the Unity Catalog linker now that the
workspace API no longer supports property updates.

Restart the backend after updating the configuration (or export
`DC43_UNITY_CATALOG_ENABLED`/`DC43_UNITY_CATALOG_SQL_DSN`). When
`link_dataset_contract` runs—either via `write_to_data_product` or through
another governance workflow—the backend now updates the matching Unity Catalog
table with:

- `dc43.contract_id`
- `dc43.contract_version`
- `dc43.dataset_version`
- Any static properties defined in the configuration (for example,
  `dc43.catalog_synced` or ownership tags)
- Matching Unity Catalog tags when `tags_enabled = true` (including any values
  from `[unity_catalog.static_tags]`). Tag keys that contain Unity-reserved
  characters such as `.`, `-`, or `/` are automatically converted to
  underscore-separated names (for example, `dc43.contract_id` becomes
  `dc43_contract_id`).

To clear those test tags later, run the inverse statement through the same
Databricks SQL warehouse:

```sql
ALTER TABLE governed.analytics.orders
UNSET TBLPROPERTIES ('dc43.contract_id', 'dc43.contract_version', 'dc43.dataset_version');
```

```sql
ALTER TABLE governed.analytics.orders
UNSET TAGS ('dc43.contract_id', 'dc43.contract_version', 'dc43.dataset_version');
```

Unity Catalog reserves specific property names (for example, `owner`), so the
backend silently drops those keys and emits a warning instead of rejecting the
entire update. Likewise, if the Databricks token injected into the DSN lacks
permission to mutate a target table, the hook logs a `RuntimeWarning` and keeps
processing the governance request so dataset↔contract links continue to record
successfully.

The dataset prefix tells the backend how to extract the table name from the
dataset identifier. The default `table:` prefix works with dataset identifiers
that start with `table:`—for example `table:governed.analytics.orders`. Adjust
it if your pipelines encode Unity Catalog references differently.

Because the tagging happens in the governance backend, it does not depend on a
specific contract or data product store implementation. Whether those services
persist their catalogues in Delta Lake, PostgreSQL, Azure Files, or another
storage layer, the Unity Catalog linker only needs the dataset identifier that
arrives with the `link_dataset_contract` call. This keeps the integration
compatible with remote deployments where the contract or product descriptors are
served by HTTP backends backed by managed databases.

The Unity Catalog bridge registers as a backend hook, so the REST contracts and
client interfaces stay agnostic of Databricks-specific concerns. Pipelines and
service clients call the same governance APIs regardless of whether tagging is
enabled; the backend simply fans out the link operation to any configured hooks
such as Unity Catalog or other metadata systems you might add later. You can add
more hook builders to the `[governance]` configuration (or via the
`DC43_GOVERNANCE_LINK_BUILDERS` environment variable) without touching the
service code, which keeps alternative integrations—such as Azure Purview or
custom auditing—completely pluggable.

## 7. Next steps

- Expose the contract and data product Delta stores through shared external
  locations so other workspaces can reuse the artefacts.
- Replace the local backends with the dc43 FastAPI services (see the operations
  guide) and update the configuration file to point at the remote URLs.
- Expand static Unity Catalog tags to capture data-product tiering, residency,
  or other platform-specific classifications that need to surface alongside the
  automated `dc43.*` metadata.
