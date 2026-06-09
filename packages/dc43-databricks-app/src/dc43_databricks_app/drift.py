"""Schema drift detection: contracted ODCS properties vs live Unity Catalog columns.

Severities mirror the dc43 drafting loop semantics:

* ``breaking`` — missing column or physical type mismatch: a new contract
  version should be drafted.
* ``warning`` — required-but-nullable, or a live column the contract does not
  cover yet: review recommended.
* ``ok`` — in sync.
"""
from __future__ import annotations

import pandas as pd

from .contracts_source import ContractSummary

_TYPE_ALIASES = {
    "bigint": "bigint", "long": "bigint",
    "int": "int", "integer": "int",
    "double": "double", "float": "float",
    "string": "string", "varchar": "string",
    "timestamp": "timestamp", "timestamp_ntz": "timestamp",
    "date": "date", "boolean": "boolean", "decimal": "decimal",
}


def _norm(t: str | None) -> str:
    t = (t or "").lower().split("(")[0].strip()
    return _TYPE_ALIASES.get(t, t)


def check_drift(contract: ContractSummary, live_columns: pd.DataFrame) -> pd.DataFrame:
    """Compare contracted properties with live UC columns.

    ``live_columns``: DataFrame[column_name, data_type, is_nullable] — the shape
    returned by ``system.information_schema.columns``.
    Returns one row per finding: (column, finding, severity).
    """
    live = {
        str(r["column_name"]).lower(): r
        for _, r in live_columns.iterrows()
    }
    findings: list[tuple[str, str, str]] = []
    seen: set[str] = set()

    for prop in contract.properties:
        name = str(prop.get("name", "")).lower()
        seen.add(name)
        expected_type = _norm(prop.get("physicalType"))
        required = bool(prop.get("required", False))

        if name not in live:
            findings.append((name, "missing in live table", "breaking"))
            continue

        column_findings = 0
        actual_type = _norm(str(live[name]["data_type"]))
        if expected_type and actual_type and expected_type != actual_type:
            findings.append(
                (name, f"type drift: contract={expected_type}, live={actual_type}", "breaking")
            )
            column_findings += 1
        nullable_live = str(live[name].get("is_nullable", "YES")).upper() == "YES"
        if required and nullable_live:
            findings.append((name, "required in contract but nullable in table", "warning"))
            column_findings += 1
        if column_findings == 0:
            findings.append((name, "in sync", "ok"))

    for name in live:
        if name not in seen:
            findings.append((name, "new column not covered by contract", "warning"))

    return pd.DataFrame(findings, columns=["column", "finding", "severity"])
