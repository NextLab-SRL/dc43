# Collibra-Orchestrated Data Contract Lifecycle

This document outlines how dc43 can integrate with [Collibra Data Products](https://productresources.collibra.com/docs/collibra/latest/Content/Assets/DataProducts/co_data-product.htm) to manage the lifecycle of data contracts alongside datasets and pipelines.

## Goals

* Centralize contract ownership and approval inside Collibra while dc43 continues to enforce the resulting specifications in Spark and Delta Live Tables (DLT).
* Re-use Collibra's native notions of **Data Products**, **Ports**, and **Contracts** to model the same artefacts surfaced by dc43.
* Allow pipelines to react automatically when Collibra validates a contract (for example by re-running a pipeline to fix or update datasets).

## Conceptual Mapping

Collibra exposes "Data Contracts" as first-class objects attached to Data Product ports. They already include status management (draft, validated, deprecated, ...). dc43 models data contracts using the Bitol/ODCS schema. The two can be mapped as follows:

| dc43 artefact | Collibra object | Notes |
| --- | --- | --- |
| Contract ID (`sales.orders`) | Data Product Port (e.g. `orders-gold`) | The port groups datasets that share the same contract. |
| Contract version (`1.2.0`) | Data Contract version on the port | Versions are tracked in Collibra and surfaced via the API. |
| Draft contract (ODCS JSON) | Collibra contract in `Draft` status | Drafts originate from dc43 `write_with_contract(..., draft_on_mismatch=True)` workflows. |
| Validation status | Collibra contract workflow state (`Validated`, `Rejected`, ...) | Collibra becomes the source of truth. |
| Dataset version status | Collibra Data Product status or custom attribute | Optionally updated when the contract is promoted. |

## Interaction Flow

1. **Draft proposal**: dc43 detects a schema mismatch while writing and produces an ODCS draft. Instead of storing it in the file-based draft store, it invokes a Collibra client that registers or updates the draft contract on the appropriate port.
2. **Collibra review**: Data Stewards review the draft contract and run their validation workflow inside Collibra. They can enrich the metadata (owners, SLAs, linked policies) before marking it as `Validated`.
3. **Pipeline trigger**: A Collibra webhook or scheduled poll notifies dc43 that the contract has been validated. dc43 resolves the latest validated contract and re-runs the associated pipeline (e.g. via Databricks Jobs or DLT expectations) to enforce the new schema and update datasets.
4. **Continuous checks**: Downstream reads call `read_with_contract` with a Collibra-backed resolver to ensure consumers always use a validated version. Data quality hooks (DQ client) can persist statuses back into Collibra if desired.

## Implementation Strategy

### API Client

* **Preferred**: Implement a Collibra client that wraps their REST APIs for Data Products and Contracts. It should support authentication, search (find the correct Data Product port for a dataset), create/update contract drafts, and fetch the latest validated contract. dc43 ships with `HttpCollibraContractGateway` to cover this use case—provide a mapping of contract identifiers to `{data_product, port}` pairs and optional access tokens.
* **Fallback / Testing**: Provide a stub with an in-memory or filesystem-backed catalogue that mirrors the API surface. dc43 includes `StubCollibraContractGateway` for unit tests and local development.

Both clients should expose a small interface consumed by dc43 components:

```python
from dc43.integration.collibra import HttpCollibraContractGateway, StubCollibraContractGateway
from dc43.storage.collibra import CollibraContractStore

gateway = HttpCollibraContractGateway(
    base_url="https://collibra/api",
    token="...",  # optional bearer token
    contract_catalog={"sales.orders": ("orders-product", "gold-port")},
)
store = CollibraContractStore(gateway, default_status="Draft", status_filter="Validated")

# Use the store anywhere dc43 expects a ContractStore implementation
contract = store.latest("sales.orders")
```

### Integration Points in dc43

* **Contract resolution**: Add a `CollibraContractStore` implementing the existing store interface (`get`, `latest`, `put`, ...), delegating to the Collibra gateway. `latest(..., status="Validated")` would filter by Collibra status.
* **Draft creation**: Extend `write_with_contract`'s draft hook to call `submit_draft`. The stub store remains available for environments without Collibra.
* **Pipeline orchestration**: Introduce a lightweight service (e.g. an Airflow DAG or Databricks Job) that subscribes to Collibra webhooks. When a contract transitions to `Validated`, it triggers the downstream pipeline and optionally records dataset remediation status back into Collibra via `update_status`.
* **DQ feedback loop**: Optionally integrate the `dq_client` so validation outcomes are synced to Collibra attributes, giving stewards visibility into enforcement history.

## Operational Considerations

* **Authentication**: Collibra Cloud exposes OAuth 2.0 / personal access tokens. The integration should support secure storage (e.g. Databricks secrets) and token rotation.
* **Idempotency**: Draft submissions may occur multiple times while a pipeline evolves. Implement upsert semantics instead of blindly creating duplicates.
* **Error handling**: Failures in Collibra calls should not crash core Spark jobs. Surface warnings, log telemetry, and fall back to local enforcement where reasonable.
* **Testing**: Use the stub gateway in unit tests with fixtures mirroring Collibra payloads. Contract-parsing logic remains unchanged thanks to the shared ODCS models.

## References

* Collibra Data Products — Data Contracts: <https://productresources.collibra.com/docs/collibra/latest/Content/Assets/DataProducts/co_data-product.htm>
* Open Data Contract Standard (ODCS) 3.0.2 (used by dc43): <https://opendatacontract.org/>
