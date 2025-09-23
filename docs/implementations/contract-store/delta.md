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

Configure it via:

```python
from dc43.storage.delta import DeltaContractStore

store = DeltaContractStore(table_path="/mnt/contracts_delta")
store.put(contract)
validated = store.latest("sales.orders", status="Validated")
```

When deploying in Unity Catalog, grant the table to the teams that need
read/write access and integrate it with your change management workflow.
