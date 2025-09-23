# Delta Contract Store

`DeltaContractStore` keeps ODCS contracts and metadata rows inside a
Delta table. It targets teams standardising on Delta Lake or Unity
Catalog and unlocks ACID writes, SQL discovery, and time travel.

## Characteristics

* Persists contract payloads alongside metadata columns (id, version,
  status, timestamps) in a managed Delta table.
* Supports `latest(...)` resolution by semantic version or additional
  filters (for example a Collibra status synced through metadata).
* Allows governance teams to query contract inventories with standard SQL
  tooling.

### Default schema

The backing table stores each contract version as one row:

| Column | Type | Description |
| --- | --- | --- |
| `id` | STRING | Contract identifier (e.g., `sales.orders`). |
| `version` | STRING | Semantic version tag. |
| `status` | STRING | Optional lifecycle label (`Draft`, `Validated`, …). |
| `updated_at` | TIMESTAMP | Last write timestamp. |
| `payload` | STRING | Raw ODCS JSON document. |

You can extend the schema with governance-specific metadata (owners,
review ticket, steward notes) and create Z-order indexes for faster
lookups by `id`.

Configure it via:

```python
from dc43.storage.delta import DeltaContractStore

store = DeltaContractStore(table_path="/mnt/contracts_delta")
store.put(contract)
validated = store.latest("sales.orders", status="Validated")
```

When deploying in Unity Catalog, grant the table to the teams that need
read/write access and integrate it with your change management workflow.

## Operational guidance

* **SQL discovery** – expose a view that projects `id`, `version`,
  `status`, and JSON fields, enabling analysts to run `SELECT * FROM
  contracts WHERE status = 'Validated'`.
* **Time travel** – leverage Delta's versioning to audit previous contract
  payloads (`DESCRIBE HISTORY contracts`).
* **Ingestion** – populate the table via `DeltaContractStore.put(contract)`
  or use COPY INTO/Spark streaming if another system (e.g., Git) feeds
  contract updates.
