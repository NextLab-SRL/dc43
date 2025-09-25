# Demo pipeline scenarios

The demo application exposes a handful of pre-baked Spark pipeline runs to highlight how dc43 components interact. This guide summarises what each scenario exercises and how the new write-violation strategies route data when quality checks fail.

## Runtime building blocks

Each scenario executes the same high-level flow:

```mermaid
graph TD
    Orders[orders:1.1.0] --> Join
    Customers[customers:1.0.0] --> Join
    Join --> Align[Schema align + contract validation]
    Align --> Strategy{Violation strategy}
    Strategy -->|full batch| Output[orders_enriched (includes violations)]
    Strategy -->|valid| Valid[orders_enriched::valid]
    Strategy -->|reject| Reject[orders_enriched::reject]
    Strategy --> Governance[Stub DQ governance]
    Governance --> Registry[Demo dataset registry]
```

* **Orders** and **Customers** are validated against their contracts on read.
* The joined dataframe is aligned to the target contract before writing.
* The **violation strategy** decides how to persist results when validation raises warnings or failed expectations.
* Strategies may keep the contracted dataset even when violations exist so that consumers can audit the full batch alongside any derived splits.
* Governance replays the validation outcome, submits metrics, and records draft contracts when necessary.

## Scenario catalogue

| Scenario | What it shows | Strategy | Contracts & versions | Outputs and write timing |
| --- | --- | --- | --- | --- |
| **No contract provided** | Schema validation rejects writes when no contract accompanies an enforced run. | No-op (default) | Inputs `orders:1.1.0` and `customers:1.0.0`; no output contract or draft is created. | Write blocked before persistence, so dataset `result-no-existing-contract` never records a version. |
| **Existing contract OK** | Baseline happy path using `orders_enriched:1.0.0`. | No-op (default) | Inputs `orders:1.1.0`, `customers:1.0.0`; output contract `orders_enriched:1.0.0` remains active. | After post-write validation succeeds the pipeline writes `orders_enriched` version `1.0.0` (subsequent runs auto-increment the patch version) containing the full batch. |
| **Existing contract fails DQ** | Expectation `amount > 100` fails; examples and a draft `orders_enriched:1.2.0` are recorded before the write is blocked. | No-op (default) | Inputs `orders:1.1.0`, `customers:1.0.0`; validation targets `orders_enriched:1.1.0` and proposes draft `orders_enriched:1.2.0`. | Post-write validation fails, preventing any dataset version from being created. |
| **Contract fails schema and DQ** | Simultaneous schema drift and expectation failures prompting a draft contract. | No-op (default) | Inputs `orders:1.1.0`, `customers:1.0.0`; validation targets `orders_enriched:2.0.0` and proposes draft `orders_enriched:2.1.0`. | Schema alignment and DQ failures block the write before a dataset version is recorded. |
| **Split invalid rows** | Valid/reject subsets are materialised so downstream consumers can remediate bad records while auditing the original batch. The run is recorded with a **warning** because violations were detected. | `SplitWriteViolationStrategy` | Inputs `orders:1.1.0`, `customers:1.0.0`; validation targets `orders_enriched:1.1.0` and produces draft `orders_enriched:1.2.0` when rejects exist. | After post-write validation the pipeline persists `orders_enriched` version `1.0.x` (auto-incremented per run) plus `orders_enriched::valid` and `orders_enriched::reject` sharing the same version identifier. |

### Scenario breakdown

All dataset versions default to `1.0.0` the first time a scenario writes a given dataset and automatically increment the patch segment (`1.0.1`, `1.0.2`, …) on subsequent runs. When a run is blocked the planned version is skipped because nothing is persisted.

#### No contract provided
- **Write timing:** The enforcement run aborts before any data is written because the output contract is missing.
- **Contracts:** Input reads rely on `orders:1.1.0` and `customers:1.0.0`; no output contract or draft is recorded.
- **Dataset versions:** `result-no-existing-contract` would start at `1.0.0`, but no version is materialised.

#### Existing contract OK
- **Write timing:** Post-write validation succeeds and the run records an OK status.
- **Contracts:** Validates against the active contract `orders_enriched:1.0.0`.
- **Dataset versions:** Writes `orders_enriched:1.0.0` on the first run and bumps the patch version on later executions.

#### Existing contract fails DQ
- **Write timing:** The revalidation step detects violations and blocks the write.
- **Contracts:** Targets `orders_enriched:1.1.0` and stores draft `orders_enriched:1.2.0` with failed-expectation examples.
- **Dataset versions:** No dataset version is written because the run fails enforcement.

#### Contract fails schema and DQ
- **Write timing:** Schema alignment and DQ checks fail before the dataset can be persisted.
- **Contracts:** Targets contract `orders_enriched:2.0.0` and records draft `orders_enriched:2.1.0` for review.
- **Dataset versions:** No dataset version is created; the failure halts the write.

#### Split invalid rows
- **Write timing:** Post-write validation completes with warnings, allowing the job to publish all outputs.
- **Contracts:** Validates against `orders_enriched:1.1.0` and records draft `orders_enriched:1.2.0` capturing the violations.
- **Dataset versions:** Persists `orders_enriched`, `orders_enriched::valid`, and `orders_enriched::reject` with the same auto-incremented version (for example `1.0.0` on the first run).

## Split strategy walkthrough

The split scenario executes with the following configuration:

```python
{
    "name": "split",
    "include_valid": True,
    "include_reject": True,
    "write_primary_on_violation": True,
}
```

Key outcomes:

* When the quality rule `amount > 100` fails, the contracted dataset is written alongside two auxiliary datasets:
  * `orders_enriched` still reflects the full batch—including the rejected rows—so auditors can reconcile the original submission. The demo flags this in the registry with a warning badge.
  * `orders_enriched::valid` contains all rows that passed every expectation.
  * `orders_enriched::reject` captures rows that violated at least one expectation so data stewards can remediate them.
  * The demo boosts one sample order above the threshold so the valid subset always includes illustrative data.
* The validation warnings bubble up in the registry UI so readers know that auxiliary datasets exist.
* Data-quality governance evaluates each split write, persisting metrics and draft contracts per dataset so change management stays intact. The registry now records the highest violation count across every output so the summary table reflects the number of affected rows.

Want the split run to fail outright? Switch the strategy to `split-strict` which wraps the split planner with `StrictWriteViolationStrategy`. The valid/reject datasets are still written but the returned validation result has `ok=False`, causing the demo to log the run as an error.

Use this scenario as a template to plug custom strategies into your own pipelines—swap out suffixes, toggle the primary write, or specialise behaviour by subclassing `WriteViolationStrategy`.
