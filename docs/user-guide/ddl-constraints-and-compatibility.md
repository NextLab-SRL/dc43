# DDL Constraints and Strategy Compatibility

When `dc43-integrations` generates table schemas or coordinates pipeline writes with governance, it bridges the gap between **declarative Data Contracts (ODCS)**, **physical storage DDL**, and **runtime violation strategies**.

This guide covers:
1. **The Declarative Constraint Registry (`DDLConstraintRegistry`)**
2. **Fine-Grained Constraint Tuning (`DDLConstraintPolicy`)**
3. **Pre-flight Write Compatibility Analysis (`analyze_write_compatibility`)**
4. **Handling Derived Tables (Reject / Quarantine Sinks)**
5. **ODCS Governance & Shadow Columns**

---

## 1. Declarative Constraint Registry (`DDLConstraintRegistry`)

Rather than hardcoding constraints into DDL generation logic, `dc43` extracts and catalogs constraints declaratively from the Open Data Contract Standard (ODCS) schema into a `DDLConstraintRegistry`.

### Supported Constraint Types

Constraints are categorized by the `DDLConstraintType` enum:

| Constraint Type | ODCS Contract Source | DDL Representation (Delta) | Notes |
| :--- | :--- | :--- | :--- |
| `NOT_NULL` | Column `required: true` | `col_name TYPE NOT NULL` | Physical storage invariant |
| `PRIMARY_KEY` | Column `primaryKey: true` | `CONSTRAINT pk_<tbl> PRIMARY KEY (col, ...)` | Table-level constraint |
| `UNIQUE` | Column `unique: true` | `CONSTRAINT uq_<tbl>_<col> UNIQUE (col)` | Table-level constraint |
| `CHECK` | Quality rules or `customProperties.checkConstraints` | `CONSTRAINT chk_<name> CHECK (expression)` | Evaluated on append/update |

### Inspecting Constraints Programmatically

```python
from dc43_integrations.spark.ddl_constraints import DDLConstraintRegistry, DDLConstraintType

registry = DDLConstraintRegistry.from_contract(contract)

# Access all constraints
all_constraints = registry.constraints

# Query constraints by type
not_null_constraints = registry.get_by_type(DDLConstraintType.NOT_NULL)
primary_keys = registry.get_by_type(DDLConstraintType.PRIMARY_KEY)

# Query constraints by column
id_constraints = registry.get_by_column("customer_id")
```

---

## 2. Fine-Grained Constraint Tuning (`DDLConstraintPolicy`)

Historically, DDL constraint generation was an all-or-nothing boolean (`enforce_ddl_constraints`). However, real-world data pipelines often need granular control—such as disabling physical `NOT NULL` on a staging table while preserving Primary Key definitions, or relaxing constraints for specific columns.

The `DDLConstraintPolicy` provides type-safe, granular control over which constraints are generated in physical DDL.

### Built-in Policy Presets

```python
from dc43_integrations.spark.ddl_constraints import DDLConstraintPolicy

# Strict: All constraints are generated (default)
policy_strict = DDLConstraintPolicy.strict()

# Relaxed: Disables NOT NULL and Primary Keys (ideal for reject/quarantine sinks)
policy_relaxed = DDLConstraintPolicy.relaxed()

# No Nullability: Preserves PKs, Unique, and Check constraints, but removes NOT NULL
policy_no_null = DDLConstraintPolicy.no_nullability()
```

### Granular Column-Level Overrides

You can selectively disable or enable specific constraint types on individual columns using `.with_column_override()`:

```python
from dc43_integrations.spark.ddl_constraints import DDLConstraintPolicy, DDLConstraintType

policy = (
    DDLConstraintPolicy.strict()
    # Allow NULL in email during ingestion, even if the contract specifies required: true
    .with_column_override("email", DDLConstraintType.NOT_NULL, enabled=False)
    # Relax CHECK constraints for legacy_id
    .with_column_override("legacy_id", DDLConstraintType.CHECK, enabled=False)
)
```

### Using Policies in `ContractDDLBuilder`

When generating DDL directly:

```python
from dc43_integrations.spark.ddl import ContractDDLBuilder

builder = ContractDDLBuilder(
    contract=contract,
    format="delta",
    constraint_policy=policy,
)
ddl_statement = builder.build_create_table()
```

---

## 3. Pre-Flight Write Compatibility Analysis

A common issue in governed pipelines occurs when the **write violation strategy** contradicts the **physical table constraints**:

- **Example 1**: The destination table was created with `col NOT NULL`. A developer configures `enforce=False` or `FlagWriteViolationStrategy` to allow null values to pass through for audit. When Delta Lake attempts to append the row, the physical storage engine crashes with `DeltaInvariantViolationException`.
- **Example 2**: `FlagWriteViolationStrategy` appends an internal `_corrupted_data` column, but that column is not declared in the contract schema. The write fails with `AnalysisException: Cannot write to target table because schema mismatch`, violating ODCS principles by introducing ungoverned "shadow columns".

`dc43` runs a **Pre-flight Compatibility Detector** (`analyze_write_compatibility`) before executing DDL or writing data.

### Detection Scenarios

| Issue Code | Scenario | Recommended Fix |
| :--- | :--- | :--- |
| `SHADOW_COLUMN_NOT_IN_CONTRACT` | A violation strategy injects a column (e.g. `_corrupted_data`) not present in the contract. | Declare the column in the contract schema, or set `allow_shadow_columns=True` on the strategy. |
| `PHYSICAL_NOT_NULL_CONFLICT` | Target table enforces `NOT NULL`, but pipeline uses `enforce=False` or non-filtering strategy with null records. | Use `DDLConstraintPolicy.no_nullability()` on the target table, or use `SplitWriteViolationStrategy` to quarantine nulls. |
| `STRICT_REJECT_TABLE_CONFIG` | The reject sink in `SplitWriteViolationStrategy` uses strict constraints, causing bad rows to fail quarantine. | Supply `reject_constraint_policy=DDLConstraintPolicy.relaxed()` (the default behavior). |

### Compatibility Modes: `"raise"` vs `"warn"`

In `GovernanceSparkWriteRequest`, you can control how compatibility issues are handled:

```python
from dc43_integrations.spark.io.common import GovernanceSparkWriteRequest

request = GovernanceSparkWriteRequest(
    contract=contract,
    output_path="delta.`/mnt/lake/orders`",
    # "raise" (default): Abort write immediately with IncompatibleGovernanceStrategyError
    # "warn": Log structured warnings with remediation steps and proceed
    compatibility_mode="raise",
    constraint_policy=policy,
)
```

When an incompatibility is detected under `compatibility_mode="raise"`, a clear, actionable exception is raised:

```
dc43_integrations.spark.io.compatibility.IncompatibleGovernanceStrategyError:
Governance Write Incompatibility detected (2 issue(s)):
  - [SHADOW_COLUMN_NOT_IN_CONTRACT] Error: Column '_corrupted_data' added by FlagWriteViolationStrategy is not declared in the contract schema.
    Fix: Add '_corrupted_data' to your Data Contract schema, or set allow_shadow_columns=True on FlagWriteViolationStrategy.
  - [PHYSICAL_NOT_NULL_CONFLICT] Warning: Physical DDL for target table enforces NOT NULL on 2 columns: ['customer_id', 'order_date'], but violation strategy allows null records through.
    Fix: Configure target table DDL with DDLConstraintPolicy.no_nullability() or use SplitWriteViolationStrategy.
```

---

## 4. Handling Reject & Quarantine Tables (`SplitWriteViolationStrategy`)

When using `SplitWriteViolationStrategy`, records that violate data quality rules are routed to a reject table (e.g., `orders_reject`). 

Because rejected records frequently contain `NULL` values in required columns or violate primary key uniqueness, **the reject table must not enforce the official strict DDL constraints**.

`SplitWriteViolationStrategy` automatically defaults `reject_constraint_policy` to `DDLConstraintPolicy.relaxed()`:

```python
from dc43_integrations.spark.violation_strategy import SplitWriteViolationStrategy
from dc43_integrations.spark.ddl_constraints import DDLConstraintPolicy

strategy = SplitWriteViolationStrategy(
    reject_suffix="reject",
    # Automatically relaxed by default: removes NOT NULL and PRIMARY KEY constraints on the reject table
    reject_constraint_policy=DDLConstraintPolicy.relaxed(),
    # The valid table retains strict constraints
    valid_constraint_policy=DDLConstraintPolicy.strict(),
)
```

This guarantees that:
1. Valid rows are written to the main table with complete structural integrity.
2. Rejected rows are preserved in the reject table with original data types and partitioning, without failing due to storage invariants.

---

## 5. ODCS Governance Philosophy: No Shadow Columns

A core tenet of Open Data Contract Standard (ODCS) governance is that **the contract is the single source of truth for the dataset schema**. Uncontracted "shadow columns" (e.g., columns injected silently by frameworks or pipeline hacks) cause schema drift and break downstream consumers.

When using `FlagWriteViolationStrategy`:

```python
from dc43_integrations.spark.violation_strategy import FlagWriteViolationStrategy

# Best Practice: Declare _corrupted_data in the ODCS contract schema!
# schema:
#   - name: _corrupted_data
#     type: string
#     required: false
#     description: "Diagnostic flags for records failing data quality checks."
strategy = FlagWriteViolationStrategy(
    flag_column="_corrupted_data",
    allow_shadow_columns=False, # Default: requires contract declaration
)

# Alternative (un-governed legacy bypass):
strategy_legacy = FlagWriteViolationStrategy(
    flag_column="_corrupted_data",
    allow_shadow_columns=True, # Bypasses contract verification
)
```

If `_corrupted_data` is missing from the contract and `allow_shadow_columns=False`, `analyze_write_compatibility` catches the issue pre-flight and prevents runtime `AnalysisException` errors on Delta tables.
