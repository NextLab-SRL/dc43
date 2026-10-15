# dc43 Examples

This directory contains reference scripts and runnable pipeline patterns for `dc43`.

## Available Examples

### 1. Incremental Streaming Ingestion with Deduplication & Quarantine (`streaming_deduplicate_quarantine_available_now.py`)
Demonstrates how to run a governed PySpark Structured Streaming job using `write_stream_with_governance` and the built-in `DeduplicateQuarantineWriteViolationStrategy`:
- Ingestion with watermarking.
- Intra-batch deduplication via window ranking (`ROW_NUMBER()`).
- Automated routing of rejected duplicates to a quarantine table (`orders_reject`) with duplicate count and list aggregation.
- Clean batch shutdown using `trigger(availableNow=True)`.

### 2. Streaming Delta Lake Upserts (`streaming_merge_with_governance.py`)
Demonstrates how to perform governed incremental merges (upserts) from a Structured Streaming source into Delta Lake:
- Automatic target table pre-creation with strict contract DDL (types, nullability, constraints, table properties) via `ContractDDLBuilder`.
- Micro-batch merge execution with `whenMatchedUpdateAll` and `whenNotMatchedInsertAll`.
- Stream trigger handling via `availableNow=True`.

### 3. Governed Permanent Views on Databricks (`declare_with_governance_databricks.py`)
Demonstrates how to deploy a governed permanent View in Databricks Unity Catalog using `declare_with_governance`:
- Dynamic resolution of input contract tables.
- Input data quality gating before view deployment.
- Contract-compliant view creation.
