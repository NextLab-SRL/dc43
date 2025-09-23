# Filesystem Contract Store

`FSContractStore` persists ODCS contracts as JSON files under a base path
(DBFS, object storage, local filesystem). It is ideal for small teams,
local development, or Git-backed workflows where contracts are reviewed
through pull requests.

## Characteristics

* Stores each contract version as `<base_path>/<contract_id>/<version>.json`.
* Reuses standard file-system semantics for ACLs and versioning.
* Plays well with Databricks Repos or mounted volumes.
* Can be combined with object storage lifecycle policies for retention.

Configure it via:

```python
from dc43.storage.fs import FSContractStore

store = FSContractStore(base_path="/mnt/contracts")
store.put(contract)
latest = store.latest("sales.orders")
```

Document additional file-based variations (Git, S3, ADLS) in this folder
if you extend the implementation with extra capabilities.
