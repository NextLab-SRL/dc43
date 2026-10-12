"""Declarative DDL constraint model, registry and policy for Spark/Delta."""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from enum import Enum
from typing import (
    Any,
    Callable,
    Iterable,
    List,
    Mapping,
    Optional,
    Sequence,
    Set,
    Tuple,
    Union,
)

from open_data_contract_standard.model import (  # type: ignore
    DataQuality,
    OpenDataContractStandard,
    SchemaObject,
    SchemaProperty,
)

from dc43_service_backends.core.odcs import (
    custom_properties_dict,
    list_properties,
)

logger = logging.getLogger(__name__)


def _sanitize_ident(name: str) -> str:
    """Sanitize identifier by wrapping in backticks if not already wrapped."""
    clean = name.strip()
    if clean.startswith("`") and clean.endswith("`"):
        return clean
    parts = clean.split(".")
    return ".".join(f"`{p.strip('`')}`" for p in parts)


class DDLConstraintType(str, Enum):
    """Enumeration of physical DDL constraint types derivable from ODCS contracts."""

    NOT_NULL = "not_null"
    PRIMARY_KEY = "primary_key"
    UNIQUE = "unique"
    CHECK = "check"


@dataclass(frozen=True)
class DDLConstraint:
    """Base class representing a physical DDL constraint."""

    name: str
    type: DDLConstraintType
    columns: tuple[str, ...]
    source: str = "contract"

    def render_column_clause(self, column_name: str) -> Optional[str]:
        """Render column-level constraint clause (e.g. NOT NULL)."""
        return None

    def render_table_clause(self, table_name: Optional[str] = None) -> Optional[str]:
        """Render table-level constraint clause (e.g. CONSTRAINT pk PRIMARY KEY (...))."""
        return None


@dataclass(frozen=True)
class NotNullConstraint(DDLConstraint):
    """Column-level NOT NULL constraint."""

    def __init__(self, column: str, source: str = "property.required") -> None:
        super().__init__(
            name=f"nn_{column}",
            type=DDLConstraintType.NOT_NULL,
            columns=(column,),
            source=source,
        )

    @property
    def column(self) -> str:
        return self.columns[0]

    def render_column_clause(self, column_name: str) -> Optional[str]:
        if column_name == self.column:
            return "NOT NULL"
        return None


@dataclass(frozen=True)
class PrimaryKeyConstraint(DDLConstraint):
    """Table-level PRIMARY KEY constraint (Delta Lake / Unity Catalog)."""

    constraint_name: Optional[str] = None

    def __init__(
        self,
        table: str,
        columns: Sequence[str],
        constraint_name: Optional[str] = None,
        source: str = "property.primaryKey",
    ) -> None:
        cols = tuple(columns)
        tbl_identifier = (table or "table").split(".")[-1].strip("`")
        c_name = constraint_name or f"pk_{tbl_identifier}"
        object.__setattr__(self, "constraint_name", c_name)
        super().__init__(
            name=c_name,
            type=DDLConstraintType.PRIMARY_KEY,
            columns=cols,
            source=source,
        )

    def render_table_clause(self, table_name: Optional[str] = None) -> Optional[str]:
        cols_str = ", ".join(f"`{c.strip('`')}`" for c in self.columns)
        c_name = self.constraint_name or (
            f"pk_{(table_name or 'table').split('.')[-1].strip('`')}"
        )
        return f"CONSTRAINT {c_name} PRIMARY KEY ({cols_str})"


@dataclass(frozen=True)
class UniqueConstraint(DDLConstraint):
    """Table-level or column-level UNIQUE constraint (Delta Lake / Unity Catalog)."""

    constraint_name: Optional[str] = None

    def __init__(
        self,
        table: str,
        columns: Sequence[str],
        constraint_name: Optional[str] = None,
        source: str = "property.unique",
    ) -> None:
        cols = tuple(columns)
        tbl_identifier = (table or "table").split(".")[-1].strip("`")
        col_suffix = "_".join(c.strip("`") for c in cols)
        c_name = constraint_name or f"uq_{tbl_identifier}_{col_suffix}"
        object.__setattr__(self, "constraint_name", c_name)
        super().__init__(
            name=c_name,
            type=DDLConstraintType.UNIQUE,
            columns=cols,
            source=source,
        )

    def render_table_clause(self, table_name: Optional[str] = None) -> Optional[str]:
        cols_str = ", ".join(f"`{c.strip('`')}`" for c in self.columns)
        return f"CONSTRAINT {self.constraint_name} UNIQUE ({cols_str})"


@dataclass(frozen=True)
class CheckConstraint(DDLConstraint):
    """Table-level CHECK constraint (Delta Lake)."""

    expression: str = ""

    def __init__(
        self,
        name: str,
        expression: str,
        columns: Sequence[str] = (),
        source: str = "contract.quality",
    ) -> None:
        object.__setattr__(self, "expression", expression)
        super().__init__(
            name=name,
            type=DDLConstraintType.CHECK,
            columns=tuple(columns),
            source=source,
        )

    def render_table_clause(self, table_name: Optional[str] = None) -> Optional[str]:
        return f"CONSTRAINT {self.name} CHECK ({self.expression})"


@dataclass(frozen=True)
class DDLConstraintPolicy:
    """Declarative policy tuning which physical DDL constraints are enforced.

    Supports global constraint type switches, granular per-column overrides,
    and column inclusion/exclusion lists.

    Examples:
        >>> # Default: all constraints enforced
        >>> policy = DDLConstraintPolicy.strict()
        >>> # Relaxed: disable all physical constraints (e.g. for reject tables)
        >>> policy = DDLConstraintPolicy.relaxed()
        >>> # Granular: disable NOT NULL on specific column using typed enum
        >>> policy = DDLConstraintPolicy().with_column_override(
        ...     column="optional_tag",
        ...     constraint_type=DDLConstraintType.NOT_NULL,
        ...     enabled=False,
        ... )
    """

    enforce_not_null: bool = True
    enforce_primary_key: bool = True
    enforce_unique: bool = True
    enforce_checks: bool = True
    column_overrides: Mapping[str, Mapping[DDLConstraintType, bool]] = field(
        default_factory=dict
    )
    excluded_columns: tuple[str, ...] = ()
    included_columns: Optional[tuple[str, ...]] = None
    filter: Optional[Callable[[DDLConstraint], bool]] = None

    @classmethod
    def strict(cls) -> DDLConstraintPolicy:
        """Return a policy where all declared constraints are strictly enforced."""
        return cls()

    @classmethod
    def relaxed(cls) -> DDLConstraintPolicy:
        """Return a policy where all physical constraints are disabled.

        Preserves column data types, table comments and partitioning while
        relaxing physical invariants (NOT NULL, PRIMARY KEY, UNIQUE, CHECK).
        Ideal for reject and quarantine tables.
        """
        return cls(
            enforce_not_null=False,
            enforce_primary_key=False,
            enforce_unique=False,
            enforce_checks=False,
        )

    @classmethod
    def no_nullability(cls) -> DDLConstraintPolicy:
        """Return a policy where NOT NULL constraints are disabled but PKs/checks are kept."""
        return cls(enforce_not_null=False)

    @classmethod
    def from_spec(
        cls, spec: Optional[Union[bool, DDLConstraintPolicy, Mapping[str, Any]]]
    ) -> DDLConstraintPolicy:
        """Coerce boolean, mapping or policy into a DDLConstraintPolicy instance."""
        if spec is None or spec is True:
            return cls.strict()
        if spec is False:
            return cls.relaxed()
        if isinstance(spec, DDLConstraintPolicy):
            return spec
        if isinstance(spec, Mapping):
            return cls(**dict(spec))
        raise TypeError(f"Invalid constraint policy specification: {spec!r}")

    def with_column_override(
        self,
        column: str,
        constraint_type: DDLConstraintType,
        enabled: bool,
    ) -> DDLConstraintPolicy:
        """Return a copy of this policy with a column-level constraint override applied."""
        new_overrides = {col: dict(types) for col, types in self.column_overrides.items()}
        col_dict = new_overrides.setdefault(column, {})
        col_dict[constraint_type] = enabled

        return DDLConstraintPolicy(
            enforce_not_null=self.enforce_not_null,
            enforce_primary_key=self.enforce_primary_key,
            enforce_unique=self.enforce_unique,
            enforce_checks=self.enforce_checks,
            column_overrides=new_overrides,
            excluded_columns=self.excluded_columns,
            included_columns=self.included_columns,
            filter=self.filter,
        )

    def is_constraint_enabled(self, constraint: DDLConstraint) -> bool:
        """Evaluate whether a specific constraint should be enforced under this policy."""
        # 1. Custom filter predicate
        if self.filter is not None and not self.filter(constraint):
            return False

        # 2. Check per-column overrides
        for col in constraint.columns:
            if col in self.column_overrides:
                overrides = self.column_overrides[col]
                if constraint.type in overrides:
                    return overrides[constraint.type]

        # 3. Column-level inclusions and exclusions
        if constraint.columns:
            if self.included_columns is not None:
                if not any(col in self.included_columns for col in constraint.columns):
                    return False
            if any(col in self.excluded_columns for col in constraint.columns):
                return False

        # 4. Global type switches
        if constraint.type == DDLConstraintType.NOT_NULL:
            return self.enforce_not_null
        if constraint.type == DDLConstraintType.PRIMARY_KEY:
            return self.enforce_primary_key
        if constraint.type == DDLConstraintType.UNIQUE:
            return self.enforce_unique
        if constraint.type == DDLConstraintType.CHECK:
            return self.enforce_checks

        return True


class DDLConstraintRegistry:
    """Registry capturing all physical DDL constraints declared in an ODCS contract."""

    def __init__(self, constraints: Optional[Sequence[DDLConstraint]] = None) -> None:
        self._constraints: list[DDLConstraint] = list(constraints or [])

    @property
    def constraints(self) -> list[DDLConstraint]:
        return list(self._constraints)

    def add(self, constraint: DDLConstraint) -> None:
        self._constraints.append(constraint)

    @classmethod
    def from_contract(
        cls,
        contract: OpenDataContractStandard,
        *,
        schema_object: Optional[str] = None,
        table: Optional[str] = None,
    ) -> DDLConstraintRegistry:
        """Extract all physical constraints declared in an ODCS contract."""
        registry = cls()
        props: List[SchemaProperty] = []
        if schema_object:
            from dc43_core.odcs import find_schema_object

            obj = find_schema_object(contract, schema_object)
            if obj and obj.properties:
                props = list(obj.properties)
        if not props:
            props = list_properties(contract)

        tbl_name = table or (
            schema_object or getattr(contract, "id", "table")
        ).split(".")[-1]

        pk_cols: List[str] = []
        for prop in props:
            col_name = prop.name
            if not col_name:
                continue

            # 1. NOT NULL constraint
            if bool(getattr(prop, "required", False)):
                registry.add(NotNullConstraint(column=col_name))

            # 2. PRIMARY KEY constraint
            if bool(getattr(prop, "primaryKey", False)):
                pk_cols.append(col_name)

            # 3. UNIQUE constraint (ODCS v3 SchemaProperty.unique)
            if bool(getattr(prop, "unique", False)):
                registry.add(
                    UniqueConstraint(table=tbl_name, columns=(col_name,))
                )

            # 4. Column-level CHECK constraints from quality rules
            quality_rules = getattr(prop, "quality", None) or []
            if isinstance(quality_rules, Iterable) and not isinstance(
                quality_rules, (str, bytes)
            ):
                for i, dq in enumerate(quality_rules):
                    if not isinstance(dq, (DataQuality, object)):
                        continue
                    rule = getattr(dq, "rule", None)
                    query = getattr(dq, "query", None)
                    dq_name = (
                        getattr(dq, "name", None)
                        or f"chk_{tbl_name}_{col_name}_{i}"
                    )
                    if rule == "customSql" and query:
                        registry.add(
                            CheckConstraint(
                                name=dq_name,
                                expression=query,
                                columns=(col_name,),
                                source=f"property.quality.{rule}",
                            )
                        )

        # Composite or table-level PRIMARY KEY constraint
        if pk_cols:
            registry.add(
                PrimaryKeyConstraint(
                    table=tbl_name,
                    columns=tuple(pk_cols),
                )
            )

        # 5. Contract-level / customProperties check constraints
        custom_props = dict(custom_properties_dict(contract))
        checks = custom_props.get("checkConstraints")
        if isinstance(checks, Mapping):
            for name, expr in checks.items():
                if expr and isinstance(expr, str):
                    registry.add(
                        CheckConstraint(
                            name=str(name),
                            expression=expr,
                            source="contract.customProperties.checkConstraints",
                        )
                    )

        return registry

    def get_not_null_columns(self) -> Set[str]:
        """Return set of column names constrained by NOT NULL."""
        return {
            c.column
            for c in self._constraints
            if isinstance(c, NotNullConstraint)
        }

    def get_primary_keys(self) -> Tuple[str, ...]:
        """Return tuple of column names comprising the Primary Key."""
        for c in self._constraints:
            if isinstance(c, PrimaryKeyConstraint):
                return c.columns
        return ()

    def get_unique_constraints(self) -> List[UniqueConstraint]:
        """Return list of UNIQUE constraints."""
        return [c for c in self._constraints if isinstance(c, UniqueConstraint)]

    def get_check_constraints(self) -> List[CheckConstraint]:
        """Return list of CHECK constraints."""
        return [c for c in self._constraints if isinstance(c, CheckConstraint)]

    def apply_policy(
        self,
        policy: Optional[DDLConstraintPolicy] = None,
        *,
        target_format: str = "delta",
    ) -> List[DDLConstraint]:
        """Filter constraints against a DDLConstraintPolicy and target storage engine format.

        Delta Lake supports NOT NULL, PRIMARY KEY, UNIQUE and CHECK constraints.
        Non-Delta formats (e.g. Parquet, ORC, Hive) only support column-level NOT NULL.
        """
        active_policy = policy or DDLConstraintPolicy.strict()
        fmt = (target_format or "delta").lower()
        supports_table_constraints = fmt in ("delta", "iceberg")

        result: List[DDLConstraint] = []
        for c in self._constraints:
            if not active_policy.is_constraint_enabled(c):
                continue

            # Engine format compatibility filter
            if not supports_table_constraints and c.type in (
                DDLConstraintType.PRIMARY_KEY,
                DDLConstraintType.UNIQUE,
                DDLConstraintType.CHECK,
            ):
                logger.debug(
                    "Omitting table-level constraint '%s' (%s) because format '%s' does not support it.",
                    c.name,
                    c.type.value,
                    fmt,
                )
                continue

            result.append(c)

        return result


__all__ = [
    "CheckConstraint",
    "DDLConstraint",
    "DDLConstraintPolicy",
    "DDLConstraintRegistry",
    "DDLConstraintType",
    "NotNullConstraint",
    "PrimaryKeyConstraint",
    "UniqueConstraint",
]
