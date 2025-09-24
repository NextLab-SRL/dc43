# Contract Drafter Component

dc43 separates **draft generation** from long-term governance so
pipelines can propose schema updates without bypassing steward approval.
The contract drafter consumes runtime signals (schemas, quality
feedback, dataset lifecycle context) and produces Open Data Contract
Standard (ODCS) documents marked as drafts.

## Responsibilities

1. **Observe runtime metadata** such as schemas, file layouts, and
   quality verdicts when a contract mismatch is detected.
2. **Create ODCS drafts** that bump semantic versions while copying
   governance metadata from the source contract.
3. **Annotate provenance** (dataset id, version, feedback) so stewards
   understand why the draft exists and can decide whether to promote or
   reject it.
4. **Hand the draft back to governance** through a contract store or
   dedicated workflow tool (Collibra, Git-based review, etc.).

```mermaid
flowchart LR
    Runtime["Runtime enforcement"] -->|schema drift| Drafter["Contract drafter"]
    Runtime -->|DQ & dataset status| Drafter
    Drafter -->|draft proposal| Governance["Contract store / steward workflow"]
    Governance -->|validated contract| Runtime
```

## Interface & context requirements

A drafter should accept both the dataset identifiers and the latest
status returned by the data-quality component. Passing the `DQStatus`
or equivalent governance record gives the drafter context about why a
draft is being created (for example, to remediate a blocking
incompatibility). Implementations can use this context to flag when a
change addresses a known failure versus introducing a new field.

To keep governance portable, the interface avoids tying draft creation
to any single runtime. Implementations can be built for batch engines,
streaming services, or manual authoring tools as long as they produce
ODCS-compliant drafts.

## Implementation catalog

Technology-specific guides live under
[`docs/implementations/contract-drafter/`](implementations/contract-drafter/).
Document each runtime adapter—such as schema registry integrations or UI
workflows—in that folder so readers can pick the implementation that
matches their environment.
