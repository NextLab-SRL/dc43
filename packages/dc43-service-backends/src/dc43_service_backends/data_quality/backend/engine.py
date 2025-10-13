"""Contract evaluation logic that stays independent from execution engines."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, Iterable, List, Literal, Mapping, Optional

from open_data_contract_standard.model import OpenDataContractStandard  # type: ignore

from dc43_service_backends.core.odcs import list_properties
from dc43_service_clients.data_quality import ValidationResult

_TYPE_SYNONYMS: Dict[str, str] = {
    "string": "string",
    "varchar": "string",
    "bigint": "bigint",
    "long": "bigint",
    "int": "int",
    "integer": "int",
    "smallint": "smallint",
    "tinyint": "tinyint",
    "float": "float",
    "double": "double",
    "decimal": "decimal",
    "boolean": "boolean",
    "bool": "boolean",
    "date": "date",
    "timestamp": "timestamp",
    "binary": "binary",
}


def _canonical_type(name: str) -> str:
    return _TYPE_SYNONYMS.get(name.lower(), name.lower()) if name else ""


@dataclass(frozen=True)
class ExpectationSpec:
    """Description of a contract rule materialised into a metric key."""

    key: str
    rule: str
    column: Optional[str] = None
    params: Mapping[str, Any] = field(default_factory=dict)
    optional: bool = False


def expectation_specs(contract: OpenDataContractStandard) -> List[ExpectationSpec]:
    """Return metric specifications derived from the contract expectations."""

    specs: List[ExpectationSpec] = []
    for obj in contract.schema_ or []:
        for field in obj.properties or []:
            if not field.name:
                continue
            optional = not bool(field.required)
            if field.required:
                specs.append(
                    ExpectationSpec(
                        key=f"not_null_{field.name}",
                        rule="not_null",
                        column=field.name,
                        optional=optional,
                    )
                )
            if field.unique:
                specs.append(
                    ExpectationSpec(
                        key=f"unique_{field.name}",
                        rule="unique",
                        column=field.name,
                        optional=optional,
                    )
                )
            for dq in field.quality or []:
                if dq.mustBeGreaterThan is not None:
                    specs.append(
                        ExpectationSpec(
                            key=f"gt_{field.name}",
                            rule="gt",
                            column=field.name,
                            params={"threshold": dq.mustBeGreaterThan},
                            optional=optional,
                        )
                    )
                if dq.mustBeGreaterOrEqualTo is not None:
                    specs.append(
                        ExpectationSpec(
                            key=f"ge_{field.name}",
                            rule="ge",
                            column=field.name,
                            params={"threshold": dq.mustBeGreaterOrEqualTo},
                            optional=optional,
                        )
                    )
                if dq.mustBeLessThan is not None:
                    specs.append(
                        ExpectationSpec(
                            key=f"lt_{field.name}",
                            rule="lt",
                            column=field.name,
                            params={"threshold": dq.mustBeLessThan},
                            optional=optional,
                        )
                    )
                if dq.mustBeLessOrEqualTo is not None:
                    specs.append(
                        ExpectationSpec(
                            key=f"le_{field.name}",
                            rule="le",
                            column=field.name,
                            params={"threshold": dq.mustBeLessOrEqualTo},
                            optional=optional,
                        )
                    )
                if (dq.rule or "").lower() == "unique":
                    specs.append(
                        ExpectationSpec(
                            key=f"unique_{field.name}",
                            rule="unique",
                            column=field.name,
                            optional=optional,
                        )
                    )
                if (dq.rule or "").lower() == "enum" and isinstance(dq.mustBe, list):
                    specs.append(
                        ExpectationSpec(
                            key=f"enum_{field.name}",
                            rule="enum",
                            column=field.name,
                            params={"values": dq.mustBe},
                            optional=optional,
                        )
                    )
                if (dq.rule or "").lower() == "regex" and dq.mustBe:
                    specs.append(
                        ExpectationSpec(
                            key=f"regex_{field.name}",
                            rule="regex",
                            column=field.name,
                            params={"pattern": dq.mustBe},
                            optional=optional,
                        )
                    )

    for obj in contract.schema_ or []:
        for dq in obj.quality or []:
            if dq.query:
                specs.append(
                    ExpectationSpec(
                        key=dq.name or dq.rule or (obj.name or "query"),
                        rule="query",
                        column=None,
                        params={"query": dq.query, "engine": dq.engine},
                    )
                )

    # Deduplicate specs keeping the first occurrence which matches column-level
    # required expectations before optional overrides.
    unique_specs: Dict[str, ExpectationSpec] = {}
    for spec in specs:
        unique_specs.setdefault(spec.key, spec)
    return list(unique_specs.values())


def _format_expectation_violation(spec: ExpectationSpec, count: int) -> str:
    column = spec.column or "field"
    if spec.rule in {"not_null", "required"}:
        return f"column {column} contains {count} null value(s) but is required in the contract"
    if spec.rule == "unique":
        return f"column {column} has {count} duplicate value(s)"
    if spec.rule == "enum":
        allowed = spec.params.get("values")
        if isinstance(allowed, Iterable):
            allowed_str = ", ".join(map(str, allowed))
        else:
            allowed_str = str(allowed)
        return f"column {column} contains {count} value(s) outside enum [{allowed_str}]"
    if spec.rule == "regex":
        return f"column {column} contains {count} value(s) not matching regex {spec.params.get('pattern')}"
    if spec.rule == "gt":
        return f"column {column} contains {count} value(s) not greater than {spec.params.get('threshold')}"
    if spec.rule == "ge":
        return f"column {column} contains {count} value(s) below {spec.params.get('threshold')}"
    if spec.rule == "lt":
        return f"column {column} contains {count} value(s) not less than {spec.params.get('threshold')}"
    if spec.rule == "le":
        return f"column {column} contains {count} value(s) above {spec.params.get('threshold')}"
    return f"expectation {spec.key} failed {count} time(s)"


def evaluate_contract(
    contract: OpenDataContractStandard,
    *,
    schema: Mapping[str, Mapping[str, Any]] | None = None,
    metrics: Mapping[str, Any] | None = None,
    strict_types: bool = True,
    allow_extra_columns: bool = True,
    expectation_severity: Literal["error", "warning", "ignore"] = "error",
) -> ValidationResult:
    """Return a :class:`ValidationResult` derived from schema & metric payloads.

    The engine evaluates observations previously collected by an execution
    runtime.  ``schema`` is expected to describe each field using the keys
    ``odcs_type`` (canonical primitive name), ``backend_type`` (optional raw
    engine type) and ``nullable`` (boolean).  ``metrics`` should contain the
    contract-driven expectation counts emitted by the runtime.  Use
    ``expectation_severity`` to control whether expectation violations are
    treated as errors (default), downgraded to warnings, or ignored entirely.
    """

    schema_map: Dict[str, Dict[str, Any]] = {
        name: dict(info) for name, info in (schema or {}).items()
    }
    metrics_map: Dict[str, Any] = dict(metrics or {})

    errors: List[str] = []
    warnings: List[str] = []

    fields = list_properties(contract)
    field_map = {f.name: f for f in fields if f.name}

    for name, field in field_map.items():
        info = schema_map.get(name)
        if info is None:
            if field.required:
                errors.append(f"missing required column: {name}")
            else:
                warnings.append(f"missing optional column: {name}")
            continue

        observed_raw = str(
            info.get("odcs_type") or info.get("type") or info.get("backend_type") or ""
        ).lower()
        expected_raw = (field.physicalType or field.logicalType or "").lower()
        observed_type = _canonical_type(observed_raw)
        expected_type = _canonical_type(expected_raw)
        if strict_types and expected_type:
            if not observed_type:
                backend = str(info.get("backend_type") or "").lower()
                backend_type = _canonical_type(backend)
                if expected_type not in (backend_type or backend):
                    errors.append(
                        f"type mismatch for {name}: expected {expected_type}, observed {backend or 'unknown'}"
                    )
            elif observed_type != expected_type:
                observed_backend = str(info.get("backend_type") or "").lower()
                raw_match = expected_raw and expected_raw in observed_raw
                backend_match = expected_type and expected_type in observed_backend
                if not raw_match and not backend_match:
                    errors.append(
                        f"type mismatch for {name}: expected {expected_type}, observed {observed_type}"
                    )

        nullable = bool(info.get("nullable", False))
        if field.required and nullable:
            violation_keys = (
                f"violations.not_null_{name}",
                f"violations.required_{name}",
            )
            if not any(key in metrics_map for key in violation_keys):
                warnings.append(
                    f"column {name} reported nullable by runtime but violation counts were not provided"
                )

    if not allow_extra_columns and schema_map:
        extras = [name for name in schema_map.keys() if name not in field_map]
        if extras:
            warnings.append(f"extra columns present: {extras}")

    if expectation_severity not in {"error", "warning", "ignore"}:
        raise ValueError(f"Unsupported expectation severity: {expectation_severity}")

    for spec in expectation_specs(contract):
        if spec.rule == "query":
            # Query expectations produce raw measurements that governance
            # components interpret; they do not represent pass/fail metrics by
            # themselves in the engine.
            continue
        metric_key = f"violations.{spec.key}"
        metric_value = metrics_map.get(metric_key)
        if metric_value is None:
            if not spec.optional:
                warnings.append(f"missing metric for expectation {spec.key}")
            continue
        if not isinstance(metric_value, (int, float)):
            warnings.append(f"unexpected metric type for {spec.key}: {type(metric_value).__name__}")
            continue
        if metric_value > 0:
            message = _format_expectation_violation(spec, int(metric_value))
            severity = expectation_severity
            if spec.rule in {"not_null", "required", "unique"}:
                severity = "error"
            if severity == "ignore":
                continue
            if severity == "error":
                errors.append(message)
            else:
                warnings.append(message)

    return ValidationResult(
        ok=not errors,
        errors=errors,
        warnings=warnings,
        metrics=metrics_map,
        schema=schema_map,
    )


__all__ = [
    "ExpectationSpec",
    "ValidationResult",
    "evaluate_contract",
    "expectation_specs",
]
