# Filesystem Stub DQ Client

`StubDQClient` offers a lightweight implementation of the `DQClient`
protocol that stores dataset↔contract links and statuses on the
filesystem. It is designed for demos, local development, and CI
pipelines where a full governance platform is not available.

## Capabilities

* Persists compatibility entries as JSON files under the configured base
  path.
* Aggregates simple counts of expectation violations returned by the DQ
  engine.
* Supports `ok`, `warn`, `block`, and `unknown` statuses based on the
  submitted metrics.
* Can be queried by `read_with_contract` / `write_with_contract` to gate
  access to datasets.

### Storage layout

```
/mnt/dq_state/
├── links/
│   └── sales.orders.json
└── status/
    └── sales.orders/
        ├── 2024-06-01T10-00-00Z.json
        └── 2024-06-02T10-00-00Z.json
```

* `links/<dataset_id>.json` stores the latest approved contract reference.
* `status/<dataset_id>/<dataset_version>.json` captures the compatibility
  verdict and metrics for a given dataset version.

Each status file contains a payload similar to:

```json
{
  "status": "block",
  "details": {
    "violations": 3,
    "metrics": {
      "row_count": 100,
      "violations.not_null_order_id": 3
    }
  }
}
```

Set `block_on_violation=False` when initial validation runs should emit warnings
instead of blocking downstream reads.

## When to use it

Use the stub client when you need end-to-end flows without provisioning a
catalog. Because it lacks workflow features, organisations typically
replace it with a production-grade adapter once they onboard to a
metadata or observability platform.

Document additional governance adapters (Collibra, Datadog, bespoke
services, …) in this folder so platform teams can compare their
capabilities and operational trade-offs.
