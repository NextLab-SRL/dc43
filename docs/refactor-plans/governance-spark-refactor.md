# Governance-Orchestrated Spark IO Refactor Plan

## 1. Current non-governance dependencies

### 1.1 Read execution pipeline
- `BaseReadExecutor.__init__` still requires explicit contract, data-quality, and data-product clients to bootstrap reads.【F:packages/dc43-integrations/src/dc43_integrations/spark/io.py†L1453-L1490】
- `_resolve_contract` falls back to direct contract client lookups and data-product resolution when governance is available, instead of delegating the entire lookup flow to governance.【F:packages/dc43-integrations/src/dc43_integrations/spark/io.py†L1539-L1599】
- `_apply_contract` bypasses governance when a dedicated data-quality client is supplied, and only invokes governance if that client is missing, forcing callers to juggle both services.【F:packages/dc43-integrations/src/dc43_integrations/spark/io.py†L1772-L1873】
- `_register_data_product_input` performs registration through the data-product client directly after a read completes.【F:packages/dc43-integrations/src/dc43_integrations/spark/io.py†L1900-L1945】

### 1.2 Write execution pipeline
- `BaseWriteExecutor.__init__` mirrors the read executor by capturing contract, data-quality, and data-product clients up front.【F:packages/dc43-integrations/src/dc43_integrations/spark/io.py†L2653-L2701】
- Contract resolution for writes still relies on the contract and data-product clients when governance should own those responsibilities.【F:packages/dc43-integrations/src/dc43_integrations/spark/io.py†L2703-L2770】
- Output port registration is performed directly against the data-product client, leaving governance unaware of the linkage lifecycle.【F:packages/dc43-integrations/src/dc43_integrations/spark/io.py†L2940-L2973】
- Revalidation for streaming writes manually switches between the data-quality client and governance, depending on what the caller provided.【F:packages/dc43-integrations/src/dc43_integrations/spark/io.py†L2985-L3039】

### 1.3 Public read/write helpers
- Batch and streaming helpers (`read_with_contract`, `read_stream_with_contract`, `write_with_contract`, `write_stream_with_contract`, `read_from_data_product`, `write_to_data_product`, and their governance wrappers) all expose optional contract, data-quality, and data-product client parameters, encouraging mixed usage instead of a single governance entry point.【F:packages/dc43-integrations/src/dc43_integrations/spark/io.py†L1969-L3535】

### 1.4 Governance service limitations
- The governance client protocol only exposes contract CRUD, expectation description, assessments, and link helpers; it has no way to resolve contracts or manage data-product bindings on behalf of callers.【F:packages/dc43-service-clients/src/dc43_service_clients/governance/client/interface.py†L18-L83】
- The local governance backend proxies contract/data-quality operations but never coordinates with a data-product client, so it cannot orchestrate bindings or derive dataset locations without the Spark helpers providing extra services.【F:packages/dc43-service-backends/src/dc43_service_backends/governance/backend/local.py†L32-L205】

## 2. Target governance-first context model

### 2.1 Read context
Introduce a `GovernanceReadContext` union that covers:
- `ContractRef`: contract identifier plus optional expected version or semantic selector.
- `InputPortRef`: data-product identifier + input port (or source product/output port) used to look up the governing contract.
- Optional dataset hint overrides (format/path/table) and expected dataset identity hints for edge cases.

Governance resolves the context into a `ResolvedReadPlan` containing:
- The canonical contract (with version validation already enforced).
- Dataset identity (dataset_id + resolved version or lineage token).
- Location metadata (format/path/table/options) so Spark helpers no longer re-implement locator logic.
- Any pending data-product registrations to persist after successful reads.

### 2.2 Write context
Define a `GovernanceWriteContext` union mirroring reads:
- `ContractRef`: same as above when the caller already knows the contract.
- `OutputPortRef`: data-product identifier + output port, supporting both explicit and default port names.
- Optional dataset hints (storage location overrides, mode guidance).

Governance resolves this into a `ResolvedWritePlan` that includes:
- The governing contract and applicable version constraints.
- Dataset identity and target storage metadata (including computed write options).
- Registration intents for data-product output ports and downstream dataset links.
- Drafting preferences (auto-bump strategy, enforce/draft toggles) driven by governance configuration.

### 2.3 Pipeline context propagation
Both read and write contexts accept an optional pipeline context payload that governance merges with its stored metadata to log activity, derive draft context, and enforce policy rules without exposing auxiliary hooks to the Spark helpers.

## 3. Governance API and backend extensions

### 3.1 Client protocol surface
Augment `GovernanceServiceClient` with:
- `resolve_read_context(context: GovernanceReadContext) -> ResolvedReadPlan`.
- `resolve_write_context(context: GovernanceWriteContext) -> ResolvedWritePlan`.
- `register_read_activity(plan: ResolvedReadPlan, assessment: QualityAssessment) -> None` to persist quality state and trigger data-product input registrations.
- `register_write_activity(plan: ResolvedWritePlan, assessment: QualityAssessment) -> None` to persist validation outcomes, link datasets/contracts, and register output ports.
- Convenience shorthands like `evaluate_read(plan, observations)` and `evaluate_write(plan, observations)` that encapsulate validation, drafting, and quality feedback orchestration.

### 3.2 Backend responsibilities
Update the local (and remote) governance backends to:
- Accept an optional data-product service (or repository abstraction) so governance can resolve bindings and register ports internally.
- Implement the new resolve/evaluate/register methods, delegating to the contract, data-quality, and data-product collaborators as needed while keeping the Spark integration unaware of those details.
- Persist read/write activity (including dataset versions) in the governance store to replace the Spark-side bookkeeping.
- Handle draft proposals and dataset-contract linking automatically when policy settings demand it, exposing only the final assessment back to the Spark helpers.

### 3.3 Configuration and bootstrap
- Extend `build_backends`/`load_service_clients` so governance backends receive the data-product backend/client instance and optional configuration for context resolution (e.g., default dataset ID conventions, auto-bump rules, enforcement defaults).
- Ensure remote governance clients expose the same context resolution/evaluation endpoints, aligning REST payloads with the new context objects.

## 4. Spark IO refactor outline

1. **Centralise context resolution**: Replace direct contract/data-product lookups in `BaseReadExecutor` and `BaseWriteExecutor` with calls to `governance_service.resolve_read_context`/`resolve_write_context`. The executors should store the returned plan and stop accepting contract/data-quality/data-product service parameters.
2. **Governance-driven validation**: Remove the `_evaluate_with_service` fallback and require governance to handle validation (optionally delegating internally to its data-quality collaborator). Executors call `evaluate_read`/`evaluate_write` on the resolved plan to obtain `QualityAssessment` objects.
3. **Registration via governance**: Drop `_register_data_product_input` and inline output registration helpers; instead, invoke `register_read_activity`/`register_write_activity` after successful enforcement so governance handles data-product bindings, dataset-contract linking, and pipeline activity logging.
4. **Simplify helper signatures**: Update all public read/write wrappers (batch, streaming, contract-focused, and data-product-focused) to accept only a governance service plus a `GovernanceReadContext`/`GovernanceWriteContext` payload. Existing helper aliases (`read_with_contract`, `read_with_governance`, `read_from_data_product`, etc.) can become thin constructors for these context objects to ease migration.
5. **Streamlined streaming support**: Ensure streaming validation uses governance-provided callbacks or handles to emit observations instead of manually instantiating `StreamingObservationWriter` with separate data-quality clients. Governance should supply any streaming intervention hooks through the resolved plan or assessment metadata.

## 5. Testing, documentation, and migration considerations

- Update integration tests under `packages/dc43-integrations/tests` to exercise contract-ID and data-product port contexts using governance-only dependencies.
- Add backend/client unit tests that cover the new resolve/evaluate/register flows for both local and remote implementations.
- Refresh demos, notebooks, and guides to show the new single-client bootstrap and the context-oriented read/write API.
- Document breaking changes in all affected changelogs, highlighting the removal of direct contract/data-quality/data-product parameters from Spark helpers and the new governance capabilities callers must adopt.
- Provide migration snippets illustrating how to construct read/write contexts for common scenarios (plain contract IDs, linked data-product ports, bespoke dataset overrides) to ease adoption across existing pipelines.
