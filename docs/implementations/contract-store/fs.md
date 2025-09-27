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

## Layout and operations

```
/mnt/contracts/
└── sales.orders/
    ├── 0.1.0.json
    ├── 0.2.0.json
    └── drafts/
        └── 0.2.0-draft-20240601T101500Z-1a2b3c4d.json
```

* **Version resolution** – `latest("sales.orders")` orders files
  lexicographically using semantic versioning. Drafts can be separated into a
  subfolder (as above) or a parallel base path.
* **Atomic updates** – write to a temporary file and `mv` into place to avoid
  partially written JSON on network filesystems.
* **Diff-friendly reviews** – when the base path is inside a Git repo (e.g.,
  Databricks Repos), contract changes can be code-reviewed with standard PR
  tooling.

Configure it via:

```python
from dc43.components.contract_store.impl.filesystem import FSContractStore

store = FSContractStore(base_path="/mnt/contracts")
store.put(contract)
latest = store.latest("sales.orders")
```

## Integration tips

* Use object storage ACLs or workspace permissions to restrict who can publish
  validated contracts.
* Pair the store with CI checks that validate ODCS payloads and semantic
  version bumps before merging.
* Mirror validated versions to immutable storage (e.g., an S3 bucket with
  versioning) for disaster recovery.

Document additional file-based variations (Git, S3, ADLS) in this folder if you
extend the implementation with extra capabilities.

### Governance metadata

When combined with the demo governance stub, a sibling directory named
`pipeline_activity/` persists JSON payloads describing which pipeline contexts
read or wrote each dataset version. Entries capture the triggering operation,
the resolved contract identifiers, and the caller-provided context so the demo
UI can surface provenance alongside contract drafts and compatibility status.
