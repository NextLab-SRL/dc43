# Delta Contract Store

`DeltaContractStore` keeps ODCS contracts and metadata rows inside a
Delta table. It targets teams standardising on Delta Lake or Unity
Catalog and unlocks ACID writes, SQL discovery, and time travel.

## Characteristics

* Persists contract payloads alongside metadata columns (identifier,
  version, names, fingerprints, timestamps) in a managed Delta table that
  mirrors the relational SQL backend.
* Supports `latest(...)` resolution by semantic version or additional
  filters (for example a Collibra status synced through metadata).
* Allows governance teams to query contract inventories with standard SQL
  tooling.

### Default schema

The backing table stores each contract version as one row:

| Column | Type | Description |
| --- | --- | --- |
| `contract_id` | STRING | Contract identifier (e.g., `sales.orders`). |
| `version` | STRING | Semantic version tag. |
| `name` | STRING | Human friendly display name (falls back to the identifier). |
| `description` | STRING | Optional usage description from the ODCS document. |
| `json` | STRING | Canonical ODCS JSON payload. |
| `fingerprint` | STRING | SHA-256 fingerprint for change detection. |
| `created_at` | TIMESTAMP | Timestamp for the latest write. |

You can extend the schema with governance-specific metadata (owners,
review ticket, steward notes) and create Z-order indexes for faster
lookups by `contract_id`.

The implementation reuses the same serialisation helper as the
SQL-backed store, so failover between Unity Catalog tables and regular
relational databases does not change the contract payload structure.

Configure it via:

```python
from dc43_service_backends.contracts.backend.stores import DeltaContractStore

store = DeltaContractStore(table="governance.meta.contracts")
store.put(contract)
validated = store.latest("sales.orders")
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
