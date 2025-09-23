# Data Quality Engine Component

dc43 ships runtime helpers that translate contract rules into concrete
metrics and status updates. Together they form the **data-quality
engine** that sits close to execution engines and feeds observations back
to governance.

## Responsibilities

1. **Interpret ODCS expectations** defined on schema properties and
   objects.
2. **Compute metrics** (row counts, expectation violations, custom
   queries) against the live dataset.
3. **Decorate DQ verdicts** with failure details to aid triage.
4. **Submit results** to the governance-facing `DQClient` so the
   compatibility matrix stays fresh.

```mermaid
flowchart LR
    Contract["Approved contract"] --> Engine["DQ engine implementation"]
    Dataset["Dataset version"] --> Engine
    Engine --> Metrics["Metrics & verdict"]
    Metrics --> Governance["DQ governance interface"]
```

The component intentionally avoids prescribing a single execution
technology. Spark, SQL warehouses, Soda, Great Expectations, or in-house
rule engines can all satisfy the same contract as long as they emit
metrics consumable by the governance interface.

## Implementation catalog

Technology-specific guides live under
[`docs/implementations/data-quality-engine/`](implementations/data-quality-engine/):

- [Spark engine](implementations/data-quality-engine/spark.md)

Document alternative engines (Soda, Great Expectations, warehouses, ...)
in the same folder when you introduce them so users can evaluate which
runtime best matches their platform.
