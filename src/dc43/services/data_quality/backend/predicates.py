"""Helpers to project expectation specs into serialisable plans."""

from __future__ import annotations

from typing import Any, Dict, Iterable, List, Mapping

from open_data_contract_standard.model import OpenDataContractStandard  # type: ignore

from .engine import ExpectationSpec, expectation_specs


def _sql_literal(value: Any) -> str:
    if isinstance(value, str):
        escaped = value.replace("'", "\\'")
        return f"'{escaped}'"
    if value is None:
        return "NULL"
    return str(value)


def sql_predicate(spec: ExpectationSpec) -> str | None:
    """Return a Spark SQL predicate for the provided expectation spec."""

    column = spec.column
    if not column:
        return None
    if spec.rule in {"not_null", "required"}:
        return f"{column} IS NOT NULL"
    if spec.rule == "gt":
        return f"{column} > {_sql_literal(spec.params.get('threshold'))}"
    if spec.rule == "ge":
        return f"{column} >= {_sql_literal(spec.params.get('threshold'))}"
    if spec.rule == "lt":
        return f"{column} < {_sql_literal(spec.params.get('threshold'))}"
    if spec.rule == "le":
        return f"{column} <= {_sql_literal(spec.params.get('threshold'))}"
    if spec.rule == "enum":
        values = spec.params.get("values") or []
        if not isinstance(values, (list, tuple, set)):
            return None
        literals = ", ".join(_sql_literal(v) for v in values)
        return f"{column} IN ({literals})" if literals else None
    if spec.rule == "regex":
        pattern = spec.params.get("pattern")
        if pattern is None:
            return None
        pattern_str = str(pattern).replace("'", "\\'")
        return f"{column} RLIKE '{pattern_str}'"
    return None


def expectation_plan(contract: OpenDataContractStandard) -> List[Dict[str, Any]]:
    """Return serialisable expectation descriptors derived from ``contract``."""

    plan: List[Dict[str, Any]] = []
    for spec in expectation_specs(contract):
        entry: Dict[str, Any] = {
            "key": spec.key,
            "rule": spec.rule,
            "column": spec.column,
            "optional": bool(spec.optional),
        }
        if spec.params:
            entry["params"] = dict(spec.params)
        predicate = sql_predicate(spec)
        if predicate:
            entry["predicate"] = predicate
        plan.append(entry)
    return plan


def expectation_predicates_from_plan(
    plan: Iterable[Mapping[str, Any]]
) -> Dict[str, str]:
    """Return ``expectation -> predicate`` from a plan when available."""

    mapping: Dict[str, str] = {}
    for item in plan:
        key = item.get("key")
        predicate = item.get("predicate")
        if isinstance(key, str) and isinstance(predicate, str):
            mapping[key] = predicate
    return mapping


__all__ = [
    "expectation_plan",
    "expectation_predicates_from_plan",
    "sql_predicate",
]
