# Integration Layer Component

dc43 keeps governance logic decoupled from runtime execution. The
integration layer provides runtime-specific adapters (Spark, warehouses,
streaming engines, APIs, …) that bridge pipeline runs to the contract and
quality managers. Integrations do **not** compute governance outcomes
themselves—they delegate to external components and stop the pipeline
when the retrieved verdict is blocking.

## Responsibilities

1. **Resolve runtime identifiers** (paths, tables, dataset versions) and
   map them to contract ids supplied by the data contract manager.
2. **Validate and coerce data** using the retrieved contract while
   respecting enforcement flags.
3. **Ask the data quality manager** for the latest dataset status and
   forward observations when the status is unknown so the manager can
   evaluate the run through the data-quality engine.
4. **Surface governance decisions** (status, drafts) back to the runtime
   so pipelines can block, warn, or persist draft proposals alongside the
   dataset version.
5. **Expose ergonomic APIs** for orchestrators—wrapping multiple
   component calls behind a simple read/write interface.

```mermaid
flowchart TD
    Adapter["Integration adapter"] -->|fetch contract| ContractMgr["Data contract manager"]
    ContractMgr --> ContractStore["Contract store"]
    Adapter -->|status & observations| DQMgr["Data quality manager"]
    DQMgr -->|evaluate| DQEngine["Data quality engine"]
    DQMgr --> Drafter["Contract drafter"]
    DQMgr --> Governance["Compatibility matrix / steward tooling"]
    Governance -->|verdict| Adapter
```

Adapters should stay thin: they orchestrate the component interfaces
rather than re-implementing them. Implementations can target Spark, SQL
warehouses, streaming frameworks, REST services, or ELT tools.

## Implementation catalog

Technology-specific guides live under
[`docs/implementations/integration/`](implementations/integration/):

- [Spark & DLT adapter](implementations/integration/spark-dlt.md)

Document additional adapters (Snowflake, Flink, dbt, …) in the same
folder so engineering teams can adopt the runtime that matches their
platform.
