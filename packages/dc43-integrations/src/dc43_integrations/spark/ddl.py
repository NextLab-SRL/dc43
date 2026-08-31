"""Contract-driven DDL generation and table pre-creation for Spark & Delta."""

from __future__ import annotations

import logging
from typing import Any, Callable, Dict, Iterable, List, Mapping, Optional, Sequence

from open_data_contract_standard.model import OpenDataContractStandard

from dc43_service_backends.core.odcs import (
    custom_properties_dict,
    list_properties,
)
from .data_quality import spark_type_name

logger = logging.getLogger(__name__)


def _escape_sql_str(value: str) -> str:
    """Escape single quotes in SQL string literals."""
    return value.replace("'", "''")


def _sanitize_identifier(name: str) -> str:
    """Wrap identifier in backticks if not already wrapped."""
    clean = name.strip()
    if clean.startswith("`") and clean.endswith("`"):
        return clean
    # Handle composite identifiers like catalog.schema.table
    parts = clean.split(".")
    return ".".join(f"`{p.strip('`')}`" for p in parts)


class ContractDDLBuilder:
    """Builder for generating and executing DDL statements from ODCS contracts."""

    def __init__(
        self,
        contract: OpenDataContractStandard,
        *,
        table: Optional[str] = None,
        path: Optional[str] = None,
        format: Optional[str] = None,
        schema_object: Optional[str] = None,
        table_properties: Optional[Mapping[str, str]] = None,
        ddl_modifier: Optional[Callable[[str], str]] = None,
    ) -> None:
        self.contract = contract
        self.table = table
        self.path = path
        self.format = (format or "delta").lower()
        self.schema_object = schema_object
        self.table_properties = dict(table_properties or {})
        self.ddl_modifier = ddl_modifier

    def build_create_table_sql(self) -> str:
        """Generate a CREATE TABLE IF NOT EXISTS statement representing the contract schema."""
        props: List[SchemaProperty] = []
        if self.schema_object:
            from dc43_core.odcs import find_schema_object
            obj = find_schema_object(self.contract, self.schema_object)
            if obj and obj.properties:
                props = list(obj.properties)
        if not props:
            props = list_properties(self.contract)
        if not props:
            raise ValueError(f"Contract {self.contract.id} has no defined properties for DDL generation")

        if not self.table and not self.path:
            from dc43_integrations.spark.io.locators import _ref_from_contract
            c_path, c_table = _ref_from_contract(self.contract, schema_object_name=self.schema_object)
            if c_table:
                self.table = c_table
            elif c_path:
                self.path = c_path

        column_defs: List[str] = []
        pk_cols: List[str] = []
        partition_cols: List[str] = []

        for prop in props:
            col_name = prop.name
            if not col_name:
                continue

            raw_type = prop.physicalType or prop.logicalType or "string"
            col_type = spark_type_name(raw_type)

            col_def = f"`{col_name}` {col_type}"

            if bool(getattr(prop, "required", False)):
                col_def += " NOT NULL"

            desc = getattr(prop, "description", None)
            if desc:
                col_def += f" COMMENT '{_escape_sql_str(desc)}'"

            column_defs.append(col_def)

            if bool(getattr(prop, "primaryKey", False)):
                pk_cols.append(f"`{col_name}`")

            if bool(getattr(prop, "partitioned", False)):
                partition_cols.append(f"`{col_name}`")

        # Add Primary Key constraint if present (Delta Lake / Databricks Unity Catalog)
        if pk_cols and self.format in ("delta",):
            tbl_identifier = (self.table or "table").split(".")[-1].strip("`")
            column_defs.append(f"CONSTRAINT pk_{tbl_identifier} PRIMARY KEY ({', '.join(pk_cols)})")

        cols_clause = ",\n    ".join(column_defs)

        target_ref = _sanitize_identifier(self.table) if self.table else None
        location_clause = None

        if not target_ref and self.path:
            if self.format == "delta":
                target_ref = f"delta.`{self.path}`"
            else:
                # In standard Spark SQL, path-only tables for non-Delta require an identifier + LOCATION
                tbl_name = self.path.rstrip("/\\").replace("\\", "/").split("/")[-1] or "table"
                target_ref = _sanitize_identifier(tbl_name)
                location_clause = f"\nLOCATION '{_escape_sql_str(self.path)}'"
        elif self.table and self.path:
            location_clause = f"\nLOCATION '{_escape_sql_str(self.path)}'"

        if not target_ref:
            raise ValueError("Either 'table' or 'path' must be provided for DDL generation")

        ddl = f"CREATE TABLE IF NOT EXISTS {target_ref} (\n    {cols_clause}\n)\nUSING {self.format}"

        # Clustering (Delta/Databricks only) vs Partitioning
        custom_props = dict(custom_properties_dict(self.contract))
        raw_schema = getattr(self.contract, "schema_", getattr(self.contract, "schema", [])) or []
        if isinstance(raw_schema, Iterable) and not isinstance(raw_schema, (str, bytes)):
            for obj in raw_schema:
                custom_props.update(custom_properties_dict(obj))

        clustering_raw = custom_props.get("clustering")
        clustering_cols: List[str] = []
        if isinstance(clustering_raw, (list, tuple)):
            clustering_cols = [f"`{c.strip('`')}`" for c in clustering_raw if c]
        elif isinstance(clustering_raw, str) and clustering_raw:
            clustering_cols = [f"`{c.strip().strip('`')}`" for c in clustering_raw.split(",") if c.strip()]

        if clustering_cols and self.format == "delta":
            ddl += f"\nCLUSTER BY ({', '.join(clustering_cols)})"
        elif partition_cols:
            ddl += f"\nPARTITIONED BY ({', '.join(partition_cols)})"

        if location_clause:
            ddl += location_clause

        desc = getattr(self.contract, "description", None)
        if desc:
            desc_str = getattr(desc, "usage", None) or getattr(desc, "purpose", None) or (str(desc) if not isinstance(desc, (dict, object)) else "")
            if isinstance(desc, str):
                desc_str = desc
            if desc_str:
                ddl += f"\nCOMMENT '{_escape_sql_str(str(desc_str))}'"

        # Aggregate table properties
        merged_props: Dict[str, str] = {}
        contract_tbl_props = custom_props.get("tableProperties")
        if isinstance(contract_tbl_props, Mapping):
            for k, v in contract_tbl_props.items():
                merged_props[str(k)] = str(v)

        for k, v in self.table_properties.items():
            merged_props[str(k)] = str(v)

        # For non-Delta formats (e.g. parquet), omit delta.* internal properties
        if self.format != "delta":
            merged_props = {k: v for k, v in merged_props.items() if not k.lower().startswith("delta.")}

        if merged_props:
            props_str = ", ".join(f"'{_escape_sql_str(k)}' = '{_escape_sql_str(v)}'" for k, v in sorted(merged_props.items()))
            ddl += f"\nTBLPROPERTIES ({props_str})"

        if self.ddl_modifier is not None:
            ddl = self.ddl_modifier(ddl)

        return ddl

    def execute(self, spark: Any) -> None:
        """Execute the generated DDL against a Spark session."""
        ddl = self.build_create_table_sql()
        logger.info("Executing contract DDL: %s", ddl)
        spark.sql(ddl)


__all__ = ["ContractDDLBuilder"]
