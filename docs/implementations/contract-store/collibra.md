# Collibra Contract Store

`CollibraContractStore` delegates contract persistence and lifecycle to
Collibra Data Products. It is backed by the
`HttpCollibraContractAdapter`, which wraps Collibra's REST APIs for Data
Product ports and contract versions.

## Characteristics

* Uses Collibra as the source of truth for contract status (`Draft`,
  `Validated`, `Deprecated`, …).
* Supports filtering by workflow status when resolving the latest
  contract (for example `latest(..., status_filter="Validated")`).
* Shares stewardship workflows with other Collibra artefacts so data
  product owners can review drafts and approvals in a single tool.

## Contract mapping

Each contract maps to a Collibra Data Product port. Versions are created through
the REST API and inherit Collibra's workflow states:

| dc43 call | Collibra action |
| --- | --- |
| `put(contract)` | Upserts the ODCS payload as a contract version on the mapped port. |
| `latest(id, status_filter="Validated")` | Finds the highest semantic version whose workflow state matches `Validated`. |
| `list_versions(id)` | Enumerates versions returned by `GET /contracts`. |

Persist the identifier mapping (`contract_id` → `{data_product, port}`) in your
adapter configuration so dc43 can route writes and reads consistently.

## Configuration example

```python
from dc43.services.contracts.backend.stores import (
    CollibraContractStore,
    HttpCollibraContractAdapter,
)

adapter = HttpCollibraContractAdapter(
    base_url="https://collibra/api",
    token="<personal-access-token>",
    contract_catalog={
        "sales.orders": ("orders-data-product", "gold-port"),
    },
)
store = CollibraContractStore(
    adapter=adapter,
    default_status="Draft",
    status_filter="Validated",
)
```

## Operational notes

* **Workflow alignment** – mirror Collibra workflow states to dc43 status
  filters so runtimes only consume validated versions.
* **Audit trail** – rely on Collibra's built-in history to trace who promoted or
  rejected a contract version.
* **Draft handling** – combine the store with the drafter so schema drift creates
  drafts in Collibra instead of on the filesystem. Stewards can continue the
  review in their native UI.

Refer to the [Collibra orchestration guide](../data-quality-governance/collibra.md)
for architecture diagrams, webhook flows, and compatibility-matrix modelling.
The same adapter also participates in the data-quality governance stories
documented there. Compatibility aliases (`HttpCollibraContractGateway`,
`StubCollibraContractGateway`) remain available if you migrate incrementally.
