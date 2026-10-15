# dc43 Documentation

Welcome to the documentation for dc43. This repository provides a governance layer that coordinates data-quality verdicts and approvals alongside contract lifecycles.

## 1. 📖 Introduction & Core Concepts
If you are new to the project or want to understand how the components fit together:
- [dc43 Architecture](architecture.md)
- [Infrastructure & Adapters (Local vs Remote)](infrastructure-and-adapters.md)
- [Stores, Telemetry & Actionable Observability](operations/stores-telemetry-and-observability.md)

## 2. 🚀 Getting Started
If you are ready to use dc43 in your pipelines or run it locally, see the guides below:
- [Spark developers: run dc43 fully locally](getting-started/spark-local.md)
- [Spark developers: consume shared dc43 services](getting-started/spark-remote.md)
- [Databricks teams: integrate dc43 with Unity Catalog](getting-started/databricks.md)

## 3. 📘 User Guide: Spark Integrations
Learn how to use the unified Spark APIs to read and write governed datasets:
- [Reading Data with Governance](user-guide/reading-data.md)
- [Writing Data with Governance](user-guide/writing-data.md)
- [Handling Violations (Enforcement & Streaming Interventions)](user-guide/handling-violations.md)

## 4. ⚙️ Operations & Deployment
For detailed information on configuring and deploying dc43 services and backends:
- [Service Backend Setup](operations/service-backend.md)
- [Stores, Telemetry & Actionable Observability](operations/stores-telemetry-and-observability.md)
- [General Configuration Reference](operations/configuration-reference.md)
- [Service Backends Configuration](operations/service-backends-configuration.md)
- [AWS ECR Setup](operations/aws-ecr-setup.md)

## 5. 🎓 Advanced Tutorials & Use Cases
End-to-end scenarios and automation guides:
- [Delta Live Tables (DLT) Integration](tutorials/spark-dlt.md)

## 6. 💡 Reference Pipelines & Examples
Runnable script templates located in [`examples/`](../examples/):
- **[Incremental Streaming Ingestion with Deduplication & Quarantine](../examples/streaming_deduplicate_quarantine_available_now.py)**: Window-based deduplication with `availableNow` trigger and quarantine routing.
- **[Streaming Delta Lake Merges](../examples/streaming_merge_with_governance.py)**: Governed CDC / stream-to-table upserts with automated table pre-creation.
- **[Governed Permanent Views](../examples/declare_with_governance_databricks.py)**: Declaring validated SQL views in Databricks Unity Catalog with input gating.

