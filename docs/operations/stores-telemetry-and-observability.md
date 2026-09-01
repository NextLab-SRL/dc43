# Stores, Telemetry, and Actionable Observability

In `dc43`, runtime data processing engines (like Apache Spark and Delta Live Tables) are strictly decoupled from governance logic, persistence stores, and observability pipelines. As data pipelines execute, `dc43` evaluates data quality expectations, enforces data contracts, and coordinates metadata across two distinct operational streams:

1. **Governance Stream (Control Plane)**: Enforces contract compliance, governs schema evolution, maintains dataset-to-contract compatibility, and injects physical catalog metadata.
2. **Observability Stream (Data Plane & Telemetry)**: Captures granular data quality metrics, validation warnings/errors, execution traces, audit logs, and operational lineage.

This guide details **where information originates**, **which sub-services manage which stores**, **what** information is consumed and published by the integration layer, **which protocols and physical schemas** are used, and **which actionable tools** should be used instead of relying solely on a passive relational database.

---

## 6-Tier Architecture Diagram (System Overview)

![dc43 6-Tier Architecture](../diagrams/stores-telemetry-architecture.svg)

```mermaid
flowchart TD
    subgraph Tier1["1. UPSTREAM PROVENANCE & BACKENDS (Where Contracts & Products Originate)"]
        direction LR
        Collibra["Collibra Governance\n• HTTPS REST / Multipart YAML\n• GET /rest/2.0/dataproducts/.../contracts"]
        DeltaCat["Delta / Unity Catalog\n• Databricks SQL / JDBC\n• SELECT payload FROM main.gov.contracts"]
        SQLStore["Relational SQL Database\n• PostgreSQL / MySQL / SQLite (DSN)\n• Table 'contracts' / 'odps_products'"]
        FSStore["Filesystem / Git Repo\n• Local / S3 / ABFS File I/O\n• ./contracts/{id}/{version}.json"]
    end

    subgraph Tier2["2. INBOUND CONSUMPTION [📥 CONSUMED AT RUNTIME] (Mandatory Dependencies)"]
        direction LR
        InContract["ODCS Contract (YAML/JSON)\n📥 CONSUMED BY INTEGRATION\n• Types, Nullability, PKs, Rules\n💥 Missing: Fatal pipeline crash"]
        InProduct["ODPS Data Product (JSON)\n📥 CONSUMED BY INTEGRATION\n• Port Bindings & SLAs\n💥 Missing: Port resolution fails"]
        InPlan["Expectation Plan (JSON)\n📥 CONSUMED BY INTEGRATION\n• Rule Predicates (regex, SQL)\n💥 Missing: No DQ computed"]
        InVerdict["Verdict & Prior dq_status\n📥 CONSUMED BY INTEGRATION\n• Status (ok/warn/block)\n💥 Missing: Circuit breaker inactive"]
    end

    subgraph Tier3["3. RUNTIME PIPELINE LAYER (Apache Spark / Databricks DLT)"]
        W["write_with_governance() / read_with_governance()"]
        Interceptors["Interceptors Chain\n(Pre-Write PII / Enrichment / Post-Write Tagging)"]
        DDLBuilder["ContractDDLBuilder\n(Hard Gated DDL: CREATE TABLE IF NOT EXISTS)"]
        ObsEngine["Observation Engine\n(In-Engine Schema & Metric Evaluation)"]
        ViolStrat["Violation Strategy Dispatcher\n(Strict Fail / Split Quarantine Rejected Records)"]
        
        W --> Interceptors --> DDLBuilder --> ObsEngine --> ViolStrat
    end

    subgraph Tier4["4. dc43 GOVERNANCE CONTROL PLANE & SUB-SERVICES (FastAPI / In-Process)"]
        direction TB
        CSBackend["ContractServiceBackend\n• GET/POST /contracts/{id}/versions/{version}\n• Reads/Writes ContractStore"]
        DPBackend["DataProductServiceBackend\n• GET/POST /data-products/{id}\n• Reads/Writes DataProductStore"]
        DQBackend["DataQualityServiceBackend & DQManager\n• POST /data-quality/evaluate\n• Executes Native / GE / Soda Engines"]
        GovBackend["GovernanceServiceBackend (Coordinator)\n• POST /governance/evaluate & read/write plans\n• Reads/Writes GovernanceStore"]
        DrafterSub["ContractDrafter\n• POST /contracts/{id}/draft on schema drift\n• Produces ODCS v1.1.0 in ContractStore"]
        UnitySub["UnityCatalogLinker (Extension Hook)\n• Prereq: Databricks SQL Warehouse DSN\n• Executes ALTER TABLE SET TAGS & TBLPROPERTIES"]
    end

    subgraph Tier5A["5A. GOVERNANCE PERSISTENCE (Control Plane Storage)"]
        direction TB
        TStatus["Table: dq_status (Latest Compliance State)\n📥 CONSUMED BY READERS (Gating Check)\n🔄 Source Boucle 2: Reader Gating\n💥 Missing: Gating impossible (corrupt data consumed)"]
        TContracts["Table: contracts / odps_products\n📥 CONSUMED BY INTEGRATION (Schema/DDL/Rules)\n🔄 Source Boucle 1: Draft Storage\n💥 Missing: Schema evolution blocked"]
        TLinks["Table: dq_dataset_contract_links\n⚙️ CONSUMED BY CONTROL PLANE (Compatibility Matrix)\n💥 Missing: Silent drift undetected"]
    end

    subgraph Tier5B["5B. OBSERVABILITY & TELEMETRY STREAM (Data Plane)"]
        direction TB
        TMetrics["Table: dq_metrics (Time-Series Metrics)\n📊 CONSUMED BY SQL DASHBOARDS\n💥 Missing: Quality trends blind"]
        TActivity["Table: dq_activity (Audit Trail Log)\n📋 CONSUMED BY REGULATORY AUDITORS (SOX/BCBS 239)\n💥 Missing: Audit failure & non-compliance"]
        OTel["OpenTelemetry Mode (OTLP gRPC 4317 / HTTP 4318)\n💥 Missing: No real-time P0 alerting"]
        OLineage["OpenLineage Mode (HTTP POST JSON 2.0.2)\n💥 Missing: No downstream DAG gating"]
    end

    subgraph Tier6A["6A. ACTIONABLE GOVERNANCE PLATFORMS"]
        CollibraApprove["Collibra / DataHub\n(Human-in-the-Loop Steward Draft Approvals)"]
        UnityABAC["Unity Catalog / Purview\n(ABAC Access Gating via dc43_status='block' Tags)"]
    end

    subgraph Tier6B["6B. ACTIONABLE OBSERVABILITY & REMEDIATION"]
        APM["Datadog / Prometheus + Grafana\n(Real-Time PagerDuty & Slack P0 Alerts)"]
        LineageTool["Marquez / DataHub\n(Visual Impact Blast-Radius & Downstream DAG Gating)"]
        Events["EventBridge / Kafka / Webhooks\n(Automated Jira Tickets & Quarantine Cleansing)"]
    end

    Tier1 -->|"HTTP REST GET / SQL Query"| Tier2
    Tier2 -->|"In-Memory Context"| Tier3
    Tier3 -->|"POST /governance/evaluate (ObservationPayload)"| Tier4
    Tier4 -->|"Status Decision (ok/warn/block)"| Tier3

    Tier4 --> Tier5A
    Tier4 --> Tier5B
    Tier3 -.->|"Spans & RunEvents"| Tier5B

    Tier5A --> Tier6A
    Tier5B --> Tier6B
```

> 💡 **Visual Diagrams Available:**
> - **Architecture Overview (SVG Vector)**: [`docs/diagrams/stores-telemetry-architecture.svg`](../diagrams/stores-telemetry-architecture.svg) (Pixel-perfect vector graphic with 6 tiers and sub-services).
> - **Integration Sequence Flow (SVG Vector)**: [`docs/diagrams/stores-telemetry-sequence-flow.svg`](../diagrams/stores-telemetry-sequence-flow.svg) (Step-by-step caller sequence and data exchange flow).
> - **Interactive Architecture Canvas (.tldr)**: [`docs/diagrams/stores-telemetry-architecture.tldr`](../diagrams/stores-telemetry-architecture.tldr) (open with [tldraw.com](https://www.tldraw.com) or the VSCode tldraw extension).
> - **Interactive Sequence Canvas (.tldr)**: [`docs/diagrams/stores-telemetry-sequence-flow.tldr`](../diagrams/stores-telemetry-sequence-flow.tldr).

---

## 2. Inverted Sequence Diagram (Integration Caller Perspective)

To clearly see **what the integration calls, what it sends, what it receives, and which storage backends are involved**, here is the step-by-step lifecycle from the perspective of `dc43-integrations`:

![dc43 Integration Sequence Flow](../diagrams/stores-telemetry-sequence-flow.svg)

```mermaid
sequenceDiagram
    autonumber
    actor Spark as PySpark Pipeline<br/>(write_with_governance)
    participant CS as ContractServiceBackend<br/>(dc43-service-backends)
    participant CStore as ContractStore<br/>(Collibra / Delta / SQL / FS)
    participant Gov as GovernanceServiceBackend<br/>(Coordinator)
    participant DQ as DataQualityServiceBackend<br/>(DQManager / Engines)
    participant GovStore as GovernanceStore (Control Plane)<br/>(dq_status / dq_links)
    participant ObsStore as Telemetry & Audit Store (Data Plane)<br/>(dq_metrics / dq_activity)
    participant UC as Unity Catalog Hook<br/>(Databricks SQL Warehouse)
    participant APM as OpenTelemetry / OpenLineage<br/>(Collector / Marquez)

    Note over Spark,CStore: PHASE 1: PRE-FLIGHT RESOLUTION (Inbound Dependencies)
    Spark->>CS: GET /contracts/{id}/versions/{version}
    CS->>CStore: Query contract payload
    CStore-->>CS: ODCS Contract (YAML / JSON)
    CS-->>Spark: Return ODCS Contract (Types, Rules, DDL, tableProperties)
    
    Spark->>Gov: GET /governance/contracts/{id}/expectations
    Gov-->>Spark: Return Expectation Execution Plan (Normalized SQL rules)

    Spark->>Gov: GET /governance/status/{dataset_id}
    Gov->>GovStore: SELECT payload FROM dq_status WHERE dataset_id = :id
    GovStore-->>Gov: Latest status
    Gov-->>Spark: Prior Dataset State (Checks if upstream is blocked)

    Note over Spark: PHASE 2: IN-ENGINE EXECUTION & DDL GATING
    Spark->>Spark: 1. Pre-Write Interceptors (PII masking)
    Spark->>Spark: 2. ContractDDLBuilder: CREATE TABLE IF NOT EXISTS
    Spark->>Spark: 3. Observation Engine: Compute metrics & schema snapshot

    Note over Spark,ObsStore: PHASE 3: EVALUATION & STATE PERSISTENCE (Multi-Store Fan-Out)
    Spark->>Gov: POST /governance/evaluate (ObservationPayload)
    Gov->>DQ: Evaluate observations against contract rules
    DQ-->>Gov: ValidationResult (ok, errors, warnings)
    Gov->>Gov: CompatibilityMatrix: compute status (ok | warn | block)
    
    opt If schema drift detected & draft_on_violation=True
        Gov->>CS: POST /contracts/{id}/draft
        CS->>CStore: Save new Draft ODCS proposal (v1.1.0)
    end

    Gov->>GovStore: 1. UPDATE dq_status (Latest state for gating)
    Gov->>GovStore: 2. INSERT dq_dataset_contract_links (Compatibility matrix link)
    Gov->>ObsStore: 3. INSERT dq_metrics (Decomposed numeric time-series)
    Gov->>ObsStore: 4. INSERT dq_activity (Audit log + pipeline context)
    
    opt If Unity Catalog Tagging Enabled
        Gov->>UC: Databricks SQL DSN: ALTER TABLE SET TAGS ('dc43_status'='ok')
    end

    Gov-->>Spark: Return QualityAssessment (status: ok | warn | block)

    Note over Spark,APM: PHASE 4: ENFORCEMENT & DIRECT TELEMETRY EGRESS
    alt Status == 'ok' or 'warn'
        Spark->>Spark: Write clean records to Target Table Sink
    else Status == 'block'
        Spark->>Spark: SplitWriteViolationStrategy: Route bad records to Quarantine Table
    end

    opt Telemetry Mode: open_telemetry
        Spark->>APM: OTLP gRPC/HTTP: Emit Trace Span + dc43.validation Event
    end
    opt Telemetry Mode: open_data_lineage
        Spark->>APM: HTTP POST JSON: Emit OpenLineage 2.0.2 RunEvent
    end
```

---

## 3. Sub-Services, Storage Mapping & Consumption Matrix

The table below provides an exact mapping of each sub-service, the storage it manages, the objects it exchanges, and whether the integration library consumes or produces them:

| Sub-Service | Managed Storage / Sink | Exposed API & Method | Payload Object | Direction & Purpose |
| :--- | :--- | :--- | :--- | :--- |
| **`ContractServiceBackend`** | `ContractStore` (Table: `contracts`, Collibra, Delta, Filesystem) | `GET /contracts/{id}/versions/{version}`<br/>`POST /contracts` | `OpenDataContractStandard` (ODCS v3.1.0) | **CONSUMED by Integration**<br/>Provides schema, column types, required flags, PKs, partitioning, and DDL tableProperties. |
| **`DataProductServiceBackend`** | `DataProductStore` (Table: `data_products`, Collibra, Delta, Filesystem) | `GET /data-products/{id}/versions/{version}` | `DataProduct` (ODPS v1.0.0) | **CONSUMED by Integration**<br/>Resolves input/output port bindings to physical sink locators and version policies. |
| **`DataQualityServiceBackend`** | In-Engine Evaluators (Native, Great Expectations, Soda) | `POST /data-quality/evaluate` | In: `ObservationPayload`<br/>Out: `ValidationResult` | **CALLED by Integration**<br/>Sends Spark observations; receives rule validation results and error counts. |
| **`GovernanceServiceBackend`** | `GovernanceStore`<br/>(Tables: `dq_status`, `dq_dataset_contract_links`, `dq_metrics`, `dq_activity`) | `POST /governance/evaluate`<br/>`POST /governance/write-plan`<br/>`GET /governance/status/{dataset_id}` | `QualityAssessment`<br/>`ValidationResult`<br/>`ResolvedWritePlan` | **PRODUCED & CONSUMED**<br/>• Writes status, links, metrics, activity to DB.<br/>• Returns verdict (`ok`/`warn`/`block`) consumed by integration for circuit breaking. |
| **`ContractDrafter`** | `ContractStore` (`contracts`) | `POST /contracts/{id}/draft` | `OpenDataContractStandard` (Draft ODCS v1.1.0) | **TRIGGERED by Governance**<br/>Persists automated draft contract in `ContractStore` for human data steward approval. |
| **`UnityCatalogLinker`** | Databricks Unity Catalog Metastore | Databricks SQL Warehouse Connection | `ALTER TABLE SET TAGS`<br/>`SET TBLPROPERTIES` | **TRIGGERED by Governance**<br/>Synchronizes metastore tags (`dc43_status`) for ABAC access control. |
| **Direct Integration Telemetry** | OpenTelemetry Collector / OpenLineage Receiver | OTLP (gRPC / HTTP) / OpenLineage REST API | OTLP Span / `RunEvent` 2.0.2 | **EMITTED DIRECTLY by Integration**<br/>Streams execution spans and lineage DAGs directly to external monitoring platforms. |

---

## 4. Upstream Inbound Dependencies & Impact Matrix

If any of the inbound dependencies are missing, the pipeline degrades or halts according to the matrix below:

| Inbound Object | Serving Sub-Service & Backend | Transport Protocol | Why It Is Mandatory | 💥 What Breaks If Missing / Unreachable |
| :--- | :--- | :--- | :--- | :--- |
| **ODCS Contract Document** | `ContractServiceBackend` (`ContractStore`) | HTTP REST / Databricks SQL / Local File I/O | Specifies schema, types, nullability, PKs, partitioning, and quality rules. | **FATAL PIPELINE CRASH.** Pipeline cannot start. `ContractDDLBuilder` cannot generate hard-gated table DDL; DataFrame cannot be cast or aligned; validation assertions cannot be built. |
| **ODPS Data Product Descriptor** | `DataProductServiceBackend` (`DataProductStore`) | HTTP REST / Databricks SQL / Local File I/O | Maps high-level port bindings (`read_from_port`, `write_to_port`) to dataset locators and version policies. | **PORT LOOKUPS FAIL.** Port resolution APIs cannot identify the underlying physical table or contract ID. |
| **Expectation Execution Plan** | `GovernanceServiceBackend` (`describe_expectations`) | HTTP REST (`GET /expectations`) | Normalizes contract quality rules into engine-evaluable SQL predicates (`not_null`, `regex`, numeric bounds). | **DATA QUALITY BYPASS.** In-engine Spark metrics and assertions are not computed. |
| **Governance Verdict & Prior `dq_status`** | `GovernanceServiceBackend` (`GovernanceStore`) | HTTP REST (`POST /evaluate`, `GET /status`) | Returns compliance decision (`ok`, `warn`, `block`) and verifies if the upstream dataset is healthy. | **CIRCUIT BREAKER INACTIVE.** Pipeline cannot enforce policy; split quarantine cannot route rejected records; downstream readers cannot verify dataset health. |
| **Dataset $\leftrightarrow$ Contract Link** | `GovernanceServiceBackend` (`GovernanceStore`) | Relational SQL / Delta Lake | Binds dataset snapshot revision to an approved contract version. | **UNCHECKED SCHEMA DRIFT.** Incompatible dataset revisions write unnoticed into shared catalogs. |

---

## 5. Physical Storage Schemas (Relational & Delta Backends)

When deploying with a relational database (PostgreSQL, MySQL, SQLite) or Delta Lake tables in Databricks Unity Catalog, `dc43` structures metadata into distinct control-plane and data-plane tables:

### A. Governance Control Tables (Control Plane Storage)

```sql
-- 1. ODCS Contract Store (Managed by ContractServiceBackend)
CREATE TABLE contracts (
    id VARCHAR(255) NOT NULL,
    version VARCHAR(64) NOT NULL,
    payload TEXT NOT NULL,          -- Full ODCS v3.1.0 specification (JSON/YAML)
    status VARCHAR(32) DEFAULT 'Draft',
    created_at TIMESTAMP NOT NULL,
    PRIMARY KEY (id, version)
);

-- 2. Latest Dataset Governance Status (Managed by GovernanceServiceBackend)
CREATE TABLE dq_status (
    dataset_id VARCHAR(255) NOT NULL,
    dataset_version VARCHAR(128) NOT NULL,
    contract_id VARCHAR(255) NOT NULL,
    contract_version VARCHAR(64) NOT NULL,
    payload TEXT NOT NULL,          -- Serialized ValidationResult (verdict, errors, warnings)
    recorded_at TIMESTAMP NOT NULL,
    PRIMARY KEY (dataset_id, dataset_version)
);

-- 3. Compatibility Matrix & Links (Managed by GovernanceServiceBackend)
CREATE TABLE dq_dataset_contract_links (
    dataset_id VARCHAR(255) NOT NULL,
    dataset_version VARCHAR(128) NOT NULL,
    contract_id VARCHAR(255) NOT NULL,
    contract_version VARCHAR(64) NOT NULL,
    linked_at TIMESTAMP NOT NULL,
    PRIMARY KEY (dataset_id, dataset_version)
);
```

### B. Observability & Telemetry Tables (Data Plane Storage)

```sql
-- 4. Historical Time-Series Metric Observations (Unpacked for SQL dashboards & drift detection)
CREATE TABLE dq_metrics (
    dataset_id VARCHAR(255) NOT NULL,
    dataset_version VARCHAR(128),
    contract_id VARCHAR(255),
    contract_version VARCHAR(64),
    metric_key VARCHAR(128) NOT NULL,        -- e.g. "row_count", "null_count.order_total", "min.amount"
    metric_value TEXT,                       -- Raw string value
    metric_numeric_value DOUBLE PRECISION,   -- Coerced float for direct indexing/charting
    status_recorded_at TIMESTAMP NOT NULL
);
CREATE INDEX idx_dq_metrics_lookup ON dq_metrics(dataset_id, metric_key, status_recorded_at);

-- 5. Historical Activity Audit Trail (Immutable regulatory compliance log)
CREATE TABLE dq_activity (
    dataset_id VARCHAR(255) NOT NULL,
    dataset_version VARCHAR(128) NOT NULL,
    contract_id VARCHAR(255),
    contract_version VARCHAR(64),
    payload TEXT NOT NULL,                   -- Full serialized ValidationResult
    pipeline_context TEXT,                   -- JSON: job_name, run_id, env, trigger
    lineage_event TEXT,                      -- JSON: OpenLineage snapshot if present
    updated_at TIMESTAMP NOT NULL,
    recorded_at TIMESTAMP,
    PRIMARY KEY (dataset_id, dataset_version)
);
```

### C. Composite Stores & Multi-Store Fan-Out Architecture

In enterprise environments, governance and observability artefacts often need to land in **different specialized backends** or be **broadcast simultaneously across multiple stores** (e.g. status in Delta Lake for fast lakehouse gating, metrics in TimescaleDB/PostgreSQL for Grafana dashboards, and audit logs in S3/Filesystem for compliance archive):

```
                                      ┌──> [Backend 1: Delta Lake] ──> Status & Links (Lakehouse Gating)
                                      │
[GovernanceServiceBackend] ──(Fan-Out)─┼──> [Backend 2: PostgreSQL] ──> Metrics & Dashboards (Time-Series)
                                      │
                                      └──> [Backend 3: S3 / ADLS] ────> Immutable Audit Log (SOX Archive)
```

#### How a Composite Store Works:
1. **Fan-Out on Writes (`save_status`, `record_pipeline_event`, `link_dataset_contract`)**:
   - The Composite Store routes operations to the designated backends.
   - If a backend only implements a subset of features (or raises `NotImplementedError`), the Composite Store absorbs it safely without disrupting the pipeline.
2. **Primary / Fallback on Reads (`load_status`, `load_metrics`, `list_datasets`)**:
   - Reads query the primary responsive store registered for that route and fallback to subsequent stores if needed.
3. **Flexible Routing (`all` / `*` Catch-All)**:
   - **`all` (or `*`)**: Directs all signals/tables to the specified store by default.
   - Specific overrides (`metrics`, `activity`, `status`, `links`) redirect or fan-out individual signals to dedicated backends.

#### TOML Configuration Example:

```toml
[governance_store]
type = "composite"

# 1. Backends declarations
[governance_store.backends.lakehouse]
type = "delta"
status_table   = "main.gov.dq_status"
link_table     = "main.gov.dq_links"
activity_table = "main.gov.dq_activity"

[governance_store.backends.bi_sql]
type = "sql"
dsn = "postgresql://bi_user:pwd@postgres:5432/analytics"
metrics_table = "dq_metrics"

[governance_store.backends.audit_s3]
type = "filesystem"
root = "s3://company-sox-vault/governance-events/"

# 2. Routing table (Optional: defaults to broadcasting across all backends)
[governance_store.routes]
all      = ["lakehouse"]              # Catch-All: all signals default to Delta Lake
metrics  = ["bi_sql"]                 # Divert metrics to PostgreSQL BI
activity = ["lakehouse", "audit_s3"]  # Fan-out: persist audit logs to Delta AND S3!
```

---

## 6. Telemetry Protocols: OpenTelemetry & OpenLineage

### A. OpenTelemetry Protocol Support (gRPC vs. HTTP/Protobuf vs. HTTP/JSON)

> 💡 **Protocol Clarification:**
> OpenTelemetry in `dc43` is **not limited to gRPC**. Because `dc43` uses standard OpenTelemetry Python APIs (`opentelemetry.trace.get_tracer`), it supports **all standard OTel exporter protocols**:
>
> 1. **gRPC (`OTLP/gRPC`)**: Port `4317` (default for `opentelemetry-exporter-otlp-proto-grpc`).
> 2. **HTTP / Protobuf (`OTLP/HTTP`)**: Port `4318`, endpoint `http://<collector>:4318/v1/traces` (default for `opentelemetry-exporter-otlp-proto-http`).
> 3. **HTTP / JSON (`OTLP/HTTP`)**: Port `4318`, endpoint `http://<collector>:4318/v1/traces` (configured via `OTEL_EXPORTER_OTLP_PROTOCOL=http/json`).

#### Configuration Examples:

**For OTLP over gRPC (Port 4317):**
```bash
pip install opentelemetry-exporter-otlp-proto-grpc
export OTEL_EXPORTER_OTLP_ENDPOINT="http://otel-collector.internal:4317"
export OTEL_EXPORTER_OTLP_PROTOCOL="grpc"
export DC43_GOVERNANCE_PUBLICATION_MODE="open_telemetry"
```

**For OTLP over HTTP / Protobuf or JSON (Port 4318):**
```bash
pip install opentelemetry-exporter-otlp-proto-http
export OTEL_EXPORTER_OTLP_ENDPOINT="http://otel-collector.internal:4318/v1/traces"
export OTEL_EXPORTER_OTLP_PROTOCOL="http/protobuf"  # or "http/json"
export DC43_GOVERNANCE_PUBLICATION_MODE="open_telemetry"
```

#### Emitted OpenTelemetry Span & Structured Events:
```json
{
  "name": "dc43.integrations.governance.write",
  "kind": "SPAN_KIND_INTERNAL",
  "attributes": {
    "dc43.governance.operation": "write",
    "dc43.governance.contract.id": "sales.orders",
    "dc43.governance.contract.version": "1.0.0",
    "dc43.governance.dataset.id": "main.sales.orders",
    "dc43.governance.dataset.version": "2026-08-18T10:15:00Z",
    "dc43.governance.dataset.table": "main.sales.orders",
    "dc43.governance.validation.status": "block",
    "dc43.governance.validation.ok": false,
    "dc43.governance.validation.reason": "Rule violations exceeded threshold: 4 null values found",
    "dc43.governance.pipeline.job_name": "daily_order_ingest",
    "dc43.governance.pipeline.run_id": "c7a840e1-45f8-4e12-b2a1-9a74288b209e",
    "dc43.governance.pipeline.env": "production"
  },
  "events": [
    {
      "name": "dc43.validation",
      "attributes": {
        "status": "block",
        "ok": false,
        "errors_count": 1,
        "warnings_count": 1,
        "reason": "Rule violations exceeded threshold: 4 null values found",
        "details": "{\"errors\": [\"Field 'order_total' contains 4 null records\"]}"
      }
    }
  ]
}
```

### B. OpenLineage Protocol Support (HTTP REST)

Emitted via HTTP POST JSON (Spec 2.0.2) to Marquez, DataHub, or Collibra Lineage:

```json
{
  "eventType": "FAIL",
  "eventTime": "2026-08-18T10:15:02.124Z",
  "job": {
    "namespace": "finance",
    "name": "daily_order_ingest"
  },
  "run": {
    "runId": "c7a840e1-45f8-4e12-b2a1-9a74288b209e",
    "facets": {
      "dc43Validation": {
        "ok": false,
        "status": "block",
        "reason": "Rule violations exceeded threshold: 4 null values found",
        "errors": ["Field 'order_total' contains 4 null records"]
      }
    }
  },
  "outputs": [
    {
      "namespace": "finance",
      "name": "main.sales.orders",
      "facets": {
        "dc43Contract": {
          "contractId": "sales.orders",
          "contractVersion": "1.0.0"
        },
        "dc43DataQuality": {
          "metrics": {
            "row_count": 12500,
            "null_count.order_total": 4
          }
        }
      }
    }
  ]
}
```

---

## 7. Databricks Unity Catalog Tagging Prerequisites

To use automated Unity Catalog metadata synchronization (`ALTER TABLE SET TAGS` and `ALTER TABLE SET TBLPROPERTIES`), the following prerequisites must be met:

### Prerequisites:
1. **Compute Resource**: An active **Databricks SQL Warehouse** (Serverless, Pro, or Classic) or an active interactive Databricks cluster endpoint.
2. **Connectivity & Driver**: The `databricks-sql-connector` Python package installed in the environment where `dc43-service-backends` runs.
3. **SQLAlchemy DSN**: Configured in `dc43-service-backends.toml` or `DC43_UNITY_CATALOG_SQL_DSN`:
   ```toml
   [unity_catalog]
   enabled = true
   sql_dsn = "databricks://token:dapi...@adb-123.azuredatabricks.net?http_path=/sql/1.0/warehouses/abc123"
   tags_enabled = true
   ```
4. **Permissions**: The Databricks Service Principal or Personal Access Token (PAT) must have the following privileges on the governed catalog and tables:
   - `USE CATALOG` on the target catalog.
   - `USE SCHEMA` on the target schema.
   - `ALTER` (or `APPLY TAG`) on the target tables.

---

## 8. Why a Database is Not Actionable & Modern Alternatives

### The "Passive Database" Anti-Pattern

Storing validation logs solely in a relational table creates a passive metadata sink:
- 🛑 **No Push Alerts**: A database does not notify an engineer when a contract fails.
- 🛑 **Polling Overhead**: Discovering issues requires scheduled polling queries.
- 🛑 **No Dependency Graph**: Relational tables cannot natively render dynamic upstream/downstream DAGs.
- 🛑 **No Incident Automation**: A database cannot page an on-call engineer or pause an Airflow DAG.

### The Actionable Tooling Ecosystem

| Category | Recommended Tools | Actionable Workflow Triggered |
| :--- | :--- | :--- |
| **APM & Alerting** | Datadog, Prometheus + Alertmanager, Dynatrace | Triggers instant PagerDuty / Slack P0 alerts when `dc43.validation.ok == false`. |
| **Active Catalog & ABAC** | Databricks Unity Catalog, Collibra, DataHub | `dc43_status='block'` tags block BI users and analysts from querying unverified tables. |
| **Operational Lineage** | Marquez, DataHub, Collibra Lineage | Renders visual blast radius DAGs and pauses downstream Airflow DAGs. |
| **Incident Remediation** | Kafka, AWS EventBridge, Jira Service Desk | Automatically opens Jira tickets and triggers quarantine cleansing pipelines. |

---

## Summary & Best Practices

1. **Decouple Policy from Storage**: Use `dc43` sub-services to keep contract evaluation independent of physical storage backends.
2. **Never Treat a Database as an Alerting Mechanism**: Always pair relational governance stores with **OpenTelemetry** for real-time alerting and **OpenLineage** for dependency tracking.
3. **Choose the Right OTel Exporter Protocol**: Use gRPC (`port 4317`) for high-throughput streaming or HTTP/protobuf / HTTP/JSON (`port 4318`) for firewall-restricted cloud environments.
4. **Automate Schema Evolution**: Enable `draft_on_violation=True` to let `ContractDrafter` propose non-breaking ODCS updates directly to data stewards.
5. **Enforce Hard Gating on DDL**: Ensure physical table schemas match contracts strictly via `ContractDDLBuilder` while managing row-level quality via policy-driven violation strategies.
