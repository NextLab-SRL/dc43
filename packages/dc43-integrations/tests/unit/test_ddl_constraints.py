"""Unit tests for declarative DDL constraints, registry, and policy."""

import pytest
from open_data_contract_standard.model import (
    CustomProperty,
    DataQuality,
    Description,
    OpenDataContractStandard,
    SchemaObject,
    SchemaProperty,
)

from dc43_integrations.spark.ddl import ContractDDLBuilder
from dc43_integrations.spark.ddl_constraints import (
    CheckConstraint,
    DDLConstraintPolicy,
    DDLConstraintRegistry,
    DDLConstraintType,
    NotNullConstraint,
    PrimaryKeyConstraint,
    UniqueConstraint,
)


def make_rich_contract():
    return OpenDataContractStandard(
        version="1.0.0",
        kind="DataContract",
        apiVersion="3.0.2",
        id="test.rich_contract",
        name="Rich Contract",
        customProperties=[
            CustomProperty(
                property="checkConstraints",
                value={"chk_positive_balance": "balance >= 0"},
            )
        ],
        schema=[
            SchemaObject(
                name="accounts",
                properties=[
                    SchemaProperty(
                        name="account_id",
                        physicalType="bigint",
                        required=True,
                        primaryKey=True,
                    ),
                    SchemaProperty(
                        name="email",
                        physicalType="string",
                        required=True,
                        unique=True,
                    ),
                    SchemaProperty(
                        name="balance",
                        physicalType="double",
                        required=True,
                        quality=[
                            DataQuality(
                                rule="customSql",
                                query="balance >= 0",
                                name="chk_balance_non_negative",
                            )
                        ],
                    ),
                    SchemaProperty(
                        name="nickname",
                        physicalType="string",
                        required=False,
                    ),
                ],
            )
        ],
    )


def test_ddl_constraint_registry_extraction():
    contract = make_rich_contract()
    registry = DDLConstraintRegistry.from_contract(contract, table="accounts")

    # 1. NOT NULL constraints
    not_nulls = registry.get_not_null_columns()
    assert not_nulls == {"account_id", "email", "balance"}

    # 2. Primary Key
    pks = registry.get_primary_keys()
    assert pks == ("account_id",)

    # 3. Unique constraints
    uniques = registry.get_unique_constraints()
    assert len(uniques) == 1
    assert uniques[0].columns == ("email",)
    assert "CONSTRAINT uq_accounts_email UNIQUE (`email`)" in uniques[0].render_table_clause()

    # 4. Check constraints
    checks = registry.get_check_constraints()
    assert len(checks) >= 2
    check_exprs = {c.expression for c in checks}
    assert "balance >= 0" in check_exprs


def test_ddl_constraint_policy_strict():
    contract = make_rich_contract()
    registry = DDLConstraintRegistry.from_contract(contract, table="accounts")
    active = registry.apply_policy(DDLConstraintPolicy.strict(), target_format="delta")

    types = {c.type for c in active}
    assert DDLConstraintType.NOT_NULL in types
    assert DDLConstraintType.PRIMARY_KEY in types
    assert DDLConstraintType.UNIQUE in types
    assert DDLConstraintType.CHECK in types


def test_ddl_constraint_policy_relaxed():
    contract = make_rich_contract()
    registry = DDLConstraintRegistry.from_contract(contract, table="accounts")
    active = registry.apply_policy(DDLConstraintPolicy.relaxed(), target_format="delta")

    assert len(active) == 0


def test_ddl_constraint_policy_no_nullability():
    contract = make_rich_contract()
    registry = DDLConstraintRegistry.from_contract(contract, table="accounts")
    active = registry.apply_policy(DDLConstraintPolicy.no_nullability(), target_format="delta")

    types = {c.type for c in active}
    assert DDLConstraintType.NOT_NULL not in types
    assert DDLConstraintType.PRIMARY_KEY in types
    assert DDLConstraintType.UNIQUE in types
    assert DDLConstraintType.CHECK in types


def test_ddl_constraint_policy_column_override():
    contract = make_rich_contract()
    registry = DDLConstraintRegistry.from_contract(contract, table="accounts")

    # Surcharge chirurgicale : désactiver NOT NULL uniquement pour la colonne 'email'
    policy = DDLConstraintPolicy().with_column_override(
        column="email",
        constraint_type=DDLConstraintType.NOT_NULL,
        enabled=False,
    )
    active = registry.apply_policy(policy, target_format="delta")

    not_null_cols = {c.column for c in active if isinstance(c, NotNullConstraint)}
    assert "account_id" in not_null_cols
    assert "balance" in not_null_cols
    assert "email" not in not_null_cols  # Surchargé à False !


def test_ddl_constraint_policy_format_filter():
    contract = make_rich_contract()
    registry = DDLConstraintRegistry.from_contract(contract, table="accounts")

    # Format parquet ne doit conserver que NOT NULL
    parquet_active = registry.apply_policy(DDLConstraintPolicy.strict(), target_format="parquet")
    parquet_types = {c.type for c in parquet_active}

    assert DDLConstraintType.NOT_NULL in parquet_types
    assert DDLConstraintType.PRIMARY_KEY not in parquet_types
    assert DDLConstraintType.UNIQUE not in parquet_types
    assert DDLConstraintType.CHECK not in parquet_types


def test_ddl_builder_with_constraint_policy():
    contract = make_rich_contract()

    # 1. Builder avec politique relaxed
    builder_relaxed = ContractDDLBuilder(
        contract=contract,
        table="accounts",
        constraint_policy=DDLConstraintPolicy.relaxed(),
    )
    sql_relaxed = builder_relaxed.build_create_table_sql()
    assert "NOT NULL" not in sql_relaxed
    assert "CONSTRAINT pk_accounts PRIMARY KEY" not in sql_relaxed
    assert "CONSTRAINT uq_accounts_email UNIQUE" not in sql_relaxed

    # 2. Builder avec politique no_nullability
    builder_no_null = ContractDDLBuilder(
        contract=contract,
        table="accounts",
        constraint_policy=DDLConstraintPolicy.no_nullability(),
    )
    sql_no_null = builder_no_null.build_create_table_sql()
    assert "NOT NULL" not in sql_no_null
    assert "CONSTRAINT pk_accounts PRIMARY KEY (`account_id`)" in sql_no_null
    assert "CONSTRAINT uq_accounts_email UNIQUE (`email`)" in sql_no_null

    # 3. Builder avec extra_columns (e.g. corruption column)
    builder_extra = ContractDDLBuilder(
        contract=contract,
        table="accounts",
        extra_columns=[("_corrupted_data", "array<string>")],
    )
    sql_extra = builder_extra.build_create_table_sql()
    assert "`_corrupted_data` array<string>" in sql_extra
