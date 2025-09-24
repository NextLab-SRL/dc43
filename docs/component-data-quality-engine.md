# Data Quality Engine Component

dc43 ships a runtime-agnostic evaluation engine plus execution-specific
helpers that collect observations. The engine sits behind the **data
quality manager**: integrations forward observations to the manager when
the dataset status is unknown, the manager invokes the engine, and the
resulting verdict feeds the compatibility matrix.

## Responsibilities

1. **Interpret ODCS expectations** defined on schema properties and
   objects.
2. **Describe observation requirements** so integrations know which
   metrics must be produced for a contract (null checks, thresholds,
   enumerations, uniqueness...).
3. **Validate schema compatibility** (presence, type alignment,
   nullability) and evaluate observation payloads for expectation
   violations.
4. **Bundle runtime context**—schema snapshots, dataset identifiers,
   sampling hints—so governance tools can reproduce or explain a verdict.
5. **Expose validation results** to the governance-facing data quality
   manager so it can update the compatibility matrix and store the
   pass/block status.
6. **Optionally run inline gates** when a platform mandates local checks
   (e.g., DLT expectations). Those gates remain configurable so the
   source of truth stays within the governance tool.

```mermaid
flowchart LR
    Contract["Approved contract"] --> Engine["DQ engine"]
    Dataset["Dataset version"] --> Collector["Observation helper"]
    Collector --> Evidence["Schema + metrics"]
    Evidence --> Engine
    Engine --> Metrics["Validation result"]
    Metrics --> DQMgr["Data quality manager"]
    DQMgr --> Governance["Compatibility matrix / steward tooling"]
```

The component intentionally avoids prescribing a single execution
technology. Spark, SQL warehouses, Soda, Great Expectations, or in-house
rule engines can all satisfy the same contract as long as they emit
metrics and schema snapshots consumable by the governance interface.

## Implementation catalog

Technology-specific collectors live under
[`docs/implementations/data-quality-integration/`](implementations/data-quality-integration/):

- [Spark data-quality integration](implementations/data-quality-integration/spark.md)

Document alternative integrations (Soda, Great Expectations, warehouses, ...)
in the same folder when you introduce them so users can evaluate which
runtime best matches their platform.
