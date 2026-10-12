"""Governance compatibility validator and conflict detector for Spark writes and DDL."""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Any, List, Optional, Sequence

from open_data_contract_standard.model import OpenDataContractStandard  # type: ignore

from dc43_service_backends.core.odcs import list_properties
from dc43_integrations.spark.ddl_constraints import (
    DDLConstraintPolicy,
    DDLConstraintRegistry,
    DDLConstraintType,
    NotNullConstraint,
    PrimaryKeyConstraint,
)

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class CompatibilityIssue:
    """Represents an identified governance or structural incompatibility."""

    code: str
    severity: str  # "ERROR" or "WARNING"
    message: str
    resolution_hint: str
    affected_columns: tuple[str, ...] = ()


@dataclass
class CompatibilityReport:
    """Comprehensive diagnostic report produced by analyze_write_compatibility."""

    is_compatible: bool
    issues: list[CompatibilityIssue] = field(default_factory=list)

    def format_diagnostic(self, target_ref: Optional[str] = None) -> str:
        """Format issues into a clean, human-readable diagnostic message."""
        lines = [
            "Incompatibility detected between governance configuration, violation strategy, and target table DDL.",
            f"Target: {target_ref or 'unknown'}",
            "",
            "Identified Conflicts:",
        ]
        for i, issue in enumerate(self.issues, 1):
            lines.append(f"  [{i}] ({issue.severity}) {issue.code}:")
            lines.append(f"      {issue.message}")
            lines.append(f"      Resolution: {issue.resolution_hint}")
            lines.append("")
        return "\n".join(lines).strip()


class IncompatibleGovernanceStrategyError(Exception):
    """Raised when an incompatible governance configuration or strategy is detected."""

    def __init__(self, report: CompatibilityReport, target_ref: Optional[str] = None) -> None:
        self.report = report
        self.target_ref = target_ref
        super().__init__(report.format_diagnostic(target_ref=target_ref))


def analyze_write_compatibility(
    request: Any,
    *,
    strategy: Optional[Any] = None,
    contract: Optional[OpenDataContractStandard] = None,
    table_exists: bool = False,
    is_merge: bool = False,
    enforce: bool = True,
    observations_have_violations: bool = False,
    broken_columns: Optional[Sequence[str]] = None,
) -> CompatibilityReport:
    """Analyze write request, strategy, contract and target table for structural incompatibilities.

    Performs static checks (shadow columns, strategy vs DDL constraints) and
    dynamic runtime checks (actual violations vs physical constraints).
    """
    issues: List[CompatibilityIssue] = []
    contract = contract or getattr(request, "contract", None)
    strategy = strategy or getattr(request, "violation_strategy", None)
    table = getattr(request, "table", None)
    path = getattr(request, "path", None)
    target_fmt = (getattr(request, "format", None) or "delta").lower()
    target_ref = table or path or "unknown"

    if not contract:
        return CompatibilityReport(is_compatible=True, issues=[])

    contract_props = list_properties(contract)
    contract_prop_names = {p.name for p in contract_props if getattr(p, "name", None)}

    # ------------------------------------------------------------------
    # 1. Shadow Column Check (No undeclared shadow data in the contract)
    # ------------------------------------------------------------------
    if strategy is not None and strategy.__class__.__name__ == "FlagWriteViolationStrategy":
        flag_col = getattr(strategy, "column_name", "_corrupted_data")
        allow_shadow = bool(getattr(strategy, "allow_shadow_columns", False))

        if flag_col not in contract_prop_names and not allow_shadow:
            issues.append(
                CompatibilityIssue(
                    code="SHADOW_COLUMN_NOT_IN_CONTRACT",
                    severity="ERROR",
                    message=(
                        f"FlagWriteViolationStrategy injects column '{flag_col}', which is absent "
                        f"from contract schema '{getattr(contract, 'id', 'unknown')}'. "
                        "Writing undeclared columns creates ungoverned shadow data."
                    ),
                    resolution_hint=(
                        f"Declare '{flag_col}' (type: array<string>) in the contract schema properties, "
                        "or set allow_shadow_columns=True on FlagWriteViolationStrategy if intentionally permitted."
                    ),
                    affected_columns=(flag_col,),
                )
            )

    # ------------------------------------------------------------------
    # 2. Strategy vs Physical Invariants (NOT NULL, PRIMARY KEY)
    # ------------------------------------------------------------------
    writes_invalid_to_primary = False
    if strategy is not None:
        if strategy.__class__.__name__ == "FlagWriteViolationStrategy":
            writes_invalid_to_primary = True
        elif getattr(strategy, "write_primary_on_violation", False):
            writes_invalid_to_primary = True

    # When enforce is False and violations were observed, invalid rows will be written to the sink
    if not enforce and observations_have_violations:
        writes_invalid_to_primary = True

    if writes_invalid_to_primary and target_fmt in ("delta", "iceberg"):
        policy = DDLConstraintPolicy.from_spec(getattr(request, "constraint_policy", None))

        # Check static DDL creation plan if table does not exist
        if not table_exists:
            registry = DDLConstraintRegistry.from_contract(contract, table=table)
            active_constraints = registry.apply_policy(policy, target_format=target_fmt)
            not_null_cols = tuple(
                c.column for c in active_constraints if isinstance(c, NotNullConstraint)
            )

            if not_null_cols and policy.enforce_not_null:
                issues.append(
                    CompatibilityIssue(
                        code="INVARIANT_NOT_NULL_CONFLICT",
                        severity="ERROR",
                        message=(
                            f"Strategy '{strategy.__class__.__name__}' (or enforce=False) writes rows containing "
                            f"potential validation failures into table '{target_ref}', but table DDL enforces "
                            f"physical NOT NULL constraints on columns: {list(not_null_cols)}. "
                            "Delta Lake will reject rows containing NULLs with a DeltaInvariantViolationException."
                        ),
                        resolution_hint=(
                            "Tune DDL constraints on your write request using DDLConstraintPolicy "
                            "(e.g. constraint_policy=DDLConstraintPolicy.no_nullability() or "
                            "constraint_policy=DDLConstraintPolicy.relaxed()), or use "
                            "SplitWriteViolationStrategy to route invalid rows to a quarantine dataset."
                        ),
                        affected_columns=not_null_cols,
                    )
                )

        # Dynamic check if table already exists and data has actual nullability / rule violations
        elif table_exists and observations_have_violations and broken_columns:
            issues.append(
                CompatibilityIssue(
                    code="DYNAMIC_INVARIANT_VIOLATION_RISK",
                    severity="WARNING",
                    message=(
                        f"Data contains actual validation violations on columns {list(broken_columns)}. "
                        f"Writing these rows to existing physical table '{target_ref}' risks failing "
                        "if the table enforces physical schema constraints."
                    ),
                    resolution_hint=(
                        "Inspect target table schema constraints and verify table nullability, "
                        "or route invalid records using SplitWriteViolationStrategy."
                    ),
                    affected_columns=tuple(broken_columns),
                )
            )

    # ------------------------------------------------------------------
    # 3. Reject Table Constraints Check (SplitWriteViolationStrategy)
    # ------------------------------------------------------------------
    if strategy is not None and strategy.__class__.__name__ == "SplitWriteViolationStrategy":
        reject_policy = getattr(strategy, "reject_constraint_policy", None)
        if reject_policy is not None and isinstance(reject_policy, DDLConstraintPolicy):
            if reject_policy.enforce_not_null or reject_policy.enforce_primary_key:
                issues.append(
                    CompatibilityIssue(
                        code="REJECT_TABLE_STRICT_CONSTRAINTS",
                        severity="ERROR",
                        message=(
                            "SplitWriteViolationStrategy reject table is configured with strict "
                            "physical constraints (NOT NULL or PRIMARY KEY), which will cause rejected "
                            "rows to fail upon writing to the reject dataset."
                        ),
                        resolution_hint=(
                            "Set reject_constraint_policy=DDLConstraintPolicy.relaxed() "
                            "(the default) on SplitWriteViolationStrategy."
                        ),
                    )
                )

    has_errors = any(i.severity == "ERROR" for i in issues)
    return CompatibilityReport(is_compatible=not has_errors, issues=issues)


__all__ = [
    "CompatibilityIssue",
    "CompatibilityReport",
    "IncompatibleGovernanceStrategyError",
    "analyze_write_compatibility",
]
