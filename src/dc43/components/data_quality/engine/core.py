"""Contract evaluation logic that stays independent from execution engines."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, List, Mapping

from open_data_contract_standard.model import OpenDataContractStandard  # type: ignore

from dc43.odcs import list_properties

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


@dataclass
class ValidationResult:
    """Outcome produced by the data-quality engine.

    ``metrics`` mirrors the observations collected by execution engines while
    ``schema`` captures the field snapshot used to derive the verdict.  Both
    payloads are stored so governance adapters can forward them without having
    to recompute anything at submission time.
    """

    ok: bool
    errors: List[str]
    warnings: List[str]
    metrics: Dict[str, Any] = field(default_factory=dict)
    schema: Dict[str, Dict[str, Any]] = field(default_factory=dict)

    @property
    def details(self) -> Dict[str, Any]:
        """Structured representation combining validation observations."""

        return {
            "errors": self.errors,
            "warnings": self.warnings,
            "metrics": self.metrics,
            "schema": self.schema,
        }


def evaluate_contract(
    contract: OpenDataContractStandard,
    *,
    schema: Mapping[str, Mapping[str, Any]] | None = None,
    metrics: Mapping[str, Any] | None = None,
    strict_types: bool = True,
    allow_extra_columns: bool = True,
) -> ValidationResult:
    """Return a :class:`ValidationResult` derived from schema & metric payloads.

    The engine evaluates observations previously collected by an execution
    runtime.  ``schema`` is expected to describe each field using the keys
    ``odcs_type`` (canonical primitive name), ``backend_type`` (optional raw
    engine type) and ``nullable`` (boolean).  ``metrics`` should contain the
    contract-driven expectation counts emitted by the runtime.
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
            null_violations = None
            for key in violation_keys:
                if key in metrics_map:
                    null_violations = metrics_map.get(key)
                    break
            if null_violations is None:
                warnings.append(
                    f"column {name} reported nullable by runtime but violation counts were not provided"
                )
            elif isinstance(null_violations, (int, float)) and null_violations > 0:
                errors.append(
                    f"column {name} contains {int(null_violations)} null value(s) but is required in the contract"
                )

    if not allow_extra_columns and schema_map:
        extras = [name for name in schema_map.keys() if name not in field_map]
        if extras:
            warnings.append(f"extra columns present: {extras}")

    return ValidationResult(
        ok=not errors,
        errors=errors,
        warnings=warnings,
        metrics=metrics_map,
        schema=schema_map,
    )


__all__ = ["ValidationResult", "evaluate_contract"]
