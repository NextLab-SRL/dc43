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

## When to use it

Use the stub client when you need end-to-end flows without provisioning a
catalog. Because it lacks workflow features, organisations typically
replace it with a production-grade adapter once they onboard to a
metadata or observability platform.

Document additional governance adapters (Collibra, Datadog, bespoke
services, …) in this folder so platform teams can compare their
capabilities and operational trade-offs.
