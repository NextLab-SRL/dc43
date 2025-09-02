# dc43 — Data Contracts for Databricks (Bitol v3.0.2)

## Overview

- Purpose: Generate, store, evolve and apply data contracts in Databricks pipelines, using standard Spark IO and Delta Live Tables (DLT) where possible.
- Contract style: Bitol/ODCS v3.0.2 JSON. Maps to Spark schemas and DLT expectations.
- Core features:
  - SemVer helpers for version checks
  - Validation and auto-casting for Spark DataFrames
  - IO helpers for read/write with contract enforcement
  - Storage backends: filesystem (DBFS/UC volumes) and Delta table
- DLT helpers: build expectations from contracts
  - DQ orchestration: pluggable client interface; stub implementation provided
- Bitol/ODCS support: relies on the official `open-data-contract-standard` models (v3.0.2). No internal stubs.

## Install

- As a source lib (Databricks Repos, workspace files) or package. No hard dependencies by default; bring your own `pyspark` on Databricks clusters.

## Quickstart

1) Define a contract (ODCS typed model)

```python
from open_data_contract_standard.model import (
    OpenDataContractStandard, SchemaObject, SchemaProperty, Description
)

contract = OpenDataContractStandard(
    version="0.1.0",
    kind="DataContract",
    apiVersion="3.0.2",
    id="sales.orders",
    name="Orders",
    description=Description(usage="Orders facts"),
    schema_=[
        SchemaObject(
            name="orders",
            properties=[
                SchemaProperty(name="order_id", physicalType="bigint", required=True, unique=True),
                SchemaProperty(name="customer_id", physicalType="bigint", required=True),
                SchemaProperty(name="order_ts", physicalType="timestamp", required=True),
                SchemaProperty(name="amount", physicalType="double", required=True),
                SchemaProperty(
                    name="currency", physicalType="string", required=True,
                    logicalTypeOptions={"enum": ["EUR", "USD"]}
                ),
            ],
        )
    ],
)
```

2) Validate and write with Spark

```python
from dc43.integration.spark_io import write_with_contract

write_with_contract(
    df=orders_df,
    contract=contract,
    path="/mnt/gold/sales/orders",
    mode="append",
    enforce=True,
    auto_cast=True,
)
```

3) DLT usage (inside a pipeline notebook)

```python
import dlt
from dc43.integration.dlt_helpers import expectations_from_contract

@dlt.table(name="orders")
def orders():
    df = spark.read.stream.table("bronze.sales_orders_raw")
    exps = expectations_from_contract(contract)
    dlt.expect_all(exps)
    return df.select("order_id", "customer_id", "order_ts", "amount", "currency")
```

4) Store and resolve contracts

```python
from dc43.storage.fs import FSContractStore

store = FSContractStore(base_path="/mnt/contracts")
store.put(contract)
latest = store.latest("sales.orders")
```

5) DQ/DO orchestration on read

```python
from dc43.integration.spark_io import read_with_contract
from dc43.dq.stub import StubDQClient

dq = StubDQClient(base_path="/mnt/dq_state")
df, status = read_with_contract(
    spark,
    format="delta",
    path="/mnt/gold/sales/orders",
    contract=contract,
    expected_contract_version="==0.1.0",
    dq_client=dq,
    return_status=True,
)
print(status.status, status.reason)
```

6) Draft contract proposal on mismatch (write)

```python
from dc43.integration.spark_io import write_with_contract
from dc43.storage.fs import FSContractStore

store = FSContractStore("/mnt/contracts-drafts")
vr, draft = write_with_contract(
    df=orders_df,
    contract=contract,
    path="/mnt/gold/sales/orders",
    mode="append",
    enforce=False,                 # continue writing
    draft_on_mismatch=True,        # create a draft when schema diverges
    draft_store=store,             # persist the draft
    return_draft=True,
)
if draft:
    print("Draft created:", draft.id, draft.version)  # send to workflow
```

## Spark Flow (Mermaid)

```mermaid
flowchart TD
    subgraph Read
      U[User code / Notebook] --> RWC{read_with_contract}
      RWC --> SR["spark.read.format(...).load"]
      RWC --> EV["ensure_version(contract)"]
      EV --> VAL[validate_dataframe]
      VAL -->|ok| AC["apply_contract (cast/order)"]
      VAL -->|errors & enforce| E1[Raise]
      AC --> DF[DataFrame ready]
      RWC --> DQ{dq_client?}
      DQ -->|yes| GS["get_status(dataset@version, contract@version)"]
      GS -->|unknown/stale| CM[compute_metrics]
      CM --> SM[submit_metrics -> status]
      GS -->|up-to-date| SM
      SM -->|block & enforce| E2[Raise]
      SM -->|ok/warn| DF
    end

    DF --> T[Transformations / joins / aggregations]

    subgraph Write
      T --> WWC{write_with_contract}
      WWC --> V2[validate_dataframe]
      V2 -->|ok| AC2[apply_contract]
      V2 -->|errors & draft_on_mismatch| PD["propose draft (ODCS model)"]
      PD --> PS[draft_store.put]
      V2 -->|errors & enforce| E3[Raise]
      AC2 --> SW["spark.write.(format, options).mode.save"]
      SW --> DELTA[Delta table / UC]
    end

    classDef err fill:#ffe5e5,stroke:#ff4d4f,color:#000
    class E1,E2,E3 err
```

```

Notes

- The library uses the official ODCS package and enforces `$schema` version `3.0.2` by default (configurable via `DC43_ODCS_REQUIRED`).
- Validation focuses on practical checks: presence, types, nullability and common constraints (enum, regex, min/max, unique best-effort).
- DLT helpers translate constraints into `expect` expressions when feasible.
 - DQ orchestration: the IO wrapper checks schema vs contract and consults the DQ client. If dataset version is newer than DQ’s known version, it computes the required metrics and submits them, then enforces the resulting status if requested.

Local Dev

- Install dependencies locally (match your Databricks runtime where possible):

```bash
pip install open-data-contract-standard==3.0.2 pyspark
```

- dc43 enforces `apiVersion` via `DC43_ODCS_REQUIRED` (default `3.0.2`).

# Tests

- Install test extras and run
```bash
pip install -e ".[test]"
pytest
```

# Publishing

- Push a tag `v*` on a commit in `main`.
- The workflow `.github/workflows/release.yml` runs tests, builds the package, waits for manual approval via environment `release`, publishes to PyPI, and creates a GitHub release with autogenerated notes.
- Set `PYPI_TOKEN` secret and configure the `release` environment with required reviewers.

# License

Apache 2
