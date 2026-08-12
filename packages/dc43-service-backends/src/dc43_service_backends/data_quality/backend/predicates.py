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
    col_ref = f"`{column.replace('`', '')}`"
    if spec.rule in {"not_null", "required"}:
        return f"{col_ref} IS NOT NULL"
    if spec.rule == "missing_values":
        values = spec.params.get("values") or []
        conds = [f"{col_ref} IS NULL"]
        for v in values:
            if v is not None:
                conds.append(f"{col_ref} = {_sql_literal(v)}")
        return f"NOT ({' OR '.join(conds)})"
    if spec.rule == "gt":
        return f"{col_ref} > {_sql_literal(spec.params.get('threshold'))}"
    if spec.rule == "ge":
        return f"{col_ref} >= {_sql_literal(spec.params.get('threshold'))}"
    if spec.rule == "lt":
        return f"{col_ref} < {_sql_literal(spec.params.get('threshold'))}"
    if spec.rule == "le":
        return f"{col_ref} <= {_sql_literal(spec.params.get('threshold'))}"
    if spec.rule == "enum":
        values = spec.params.get("values") or []
        if not isinstance(values, (list, tuple, set)):
            return None
        literals = ", ".join(_sql_literal(v) for v in values)
        return f"{col_ref} IN ({literals})" if literals else None
    if spec.rule == "regex":
        pattern = spec.params.get("pattern")
        if pattern is None:
            return None
        pattern_str = str(pattern).replace("'", "\\'")
        return f"{col_ref} RLIKE '{pattern_str}'"
    if spec.rule == "exact":
        val = spec.params.get("value")
        return f"{col_ref} = {_sql_literal(val)}"
    if spec.rule == "is_null":
        return f"{col_ref} IS NULL"
    if spec.rule == "exact_format":
        fmt = spec.params.get("format")
        if not fmt:
            return None
        fmt_str = str(fmt).replace("'", "\\'")
        return f"{col_ref} IS NULL OR try_to_timestamp({col_ref}, '{fmt_str}') IS NOT NULL"
    if spec.rule == "float_format":
        dec_sep = spec.params.get("decimalSeparator", ".")
        th_sep = spec.params.get("thousandsSeparator")
        
        import re
        dec_esc = re.escape(dec_sep)
        
        if th_sep:
            th_esc = re.escape(th_sep)
            # Allow properly grouped separators (e.g. 1 234.56 or 1,234.56) or no separators (e.g. 1234.56)
            pattern = f"^[+-]?(?:(?:\\d{{1,3}}(?:{th_esc}\\d{{3}})+|\\d+)(?:{dec_esc}\\d*)?|{dec_esc}\\d+)$"
        else:
            pattern = f"^[+-]?(?:\\d+(?:{dec_esc}\\d*)?|{dec_esc}\\d+)$"
            
        pattern_str = pattern.replace("\\", "\\\\").replace("'", "\\'")
        return f"{col_ref} IS NULL OR {col_ref} RLIKE '{pattern_str}'"
    if spec.rule == "integer_format":
        th_sep = spec.params.get("thousandsSeparator")
        
        if th_sep:
            import re
            th_esc = re.escape(th_sep)
            # Allow properly grouped separators (e.g. 1 234 or 1,234) or no separators (e.g. 1234)
            pattern = f"^[+-]?(?:\\d{{1,3}}(?:{th_esc}\\d{{3}})+|\\d+)$"
        else:
            pattern = r"^[+-]?\d+$"
            
        pattern_str = pattern.replace("\\", "\\\\").replace("'", "\\'")
        return f"{col_ref} IS NULL OR {col_ref} RLIKE '{pattern_str}'"
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
