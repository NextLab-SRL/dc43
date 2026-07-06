from dc43_service_backends.data_quality.backend.engine import ExpectationSpec
from dc43_service_backends.data_quality.backend.predicates import sql_predicate


def test_sql_predicate_exact_format() -> None:
    spec = ExpectationSpec(
        key="exact_format_date_col",
        rule="exact_format",
        column="date_col",
        params={"format": "yyyy-MM-dd"},
    )
    predicate = sql_predicate(spec)
    assert predicate == "`date_col` IS NULL OR try_to_timestamp(`date_col`, 'yyyy-MM-dd') IS NOT NULL"


def test_sql_predicate_not_null() -> None:
    spec = ExpectationSpec(
        key="not_null_id",
        rule="not_null",
        column="id",
    )
    predicate = sql_predicate(spec)
    assert predicate == "`id` IS NOT NULL"


def test_sql_predicate_gt() -> None:
    spec = ExpectationSpec(
        key="gt_amount",
        rule="gt",
        column="amount",
        params={"threshold": 10.5},
    )
    predicate = sql_predicate(spec)
    assert predicate == "`amount` > 10.5"


def test_sql_predicate_enum() -> None:
    spec = ExpectationSpec(
        key="enum_status",
        rule="enum",
        column="status",
        params={"values": ["active", "pending"]},
    )
    predicate = sql_predicate(spec)
    assert predicate == "`status` IN ('active', 'pending')"


def test_sql_predicate_regex() -> None:
    spec = ExpectationSpec(
        key="regex_code",
        rule="regex",
        column="code",
        params={"pattern": r"^\d{3}$"},
    )
    predicate = sql_predicate(spec)
    assert predicate == r"`code` RLIKE '^\d{3}$'"


def test_sql_predicate_float_format() -> None:
    # 1. Default decimal separator (.), no thousands separator
    spec1 = ExpectationSpec(
        key="float_format_val1",
        rule="float_format",
        column="val1",
        params={"decimalSeparator": "."},
    )
    predicate1 = sql_predicate(spec1)
    assert predicate1 == r"`val1` IS NULL OR `val1` RLIKE '^[+-]?(?:\\d+(?:\\.\\d*)?|\\.\\d+)$'"

    # 2. Custom decimal separator (,), custom thousands separator (.)
    spec2 = ExpectationSpec(
        key="float_format_val2",
        rule="float_format",
        column="val2",
        params={"decimalSeparator": ",", "thousandsSeparator": "."},
    )
    predicate2 = sql_predicate(spec2)
    assert predicate2 == r"`val2` IS NULL OR `val2` RLIKE '^[+-]?(?:(?:\\d{1,3}(?:\\.\\d{3})+|\\d+)(?:,\\d*)?|,\\d+)$'"

    # 3. Custom decimal separator (.), custom thousands separator (space)
    spec3 = ExpectationSpec(
        key="float_format_val3",
        rule="float_format",
        column="val3",
        params={"decimalSeparator": ".", "thousandsSeparator": " "},
    )
    predicate3 = sql_predicate(spec3)
    assert predicate3 == r"`val3` IS NULL OR `val3` RLIKE '^[+-]?(?:(?:\\d{1,3}(?:\\ \\d{3})+|\\d+)(?:\\.\\d*)?|\\.\\d+)$'"



def test_expectation_plan_extracts_float_format() -> None:
    from open_data_contract_standard.model import (
        OpenDataContractStandard,
        SchemaObject,
        SchemaProperty,
    )
    from dc43_service_backends.data_quality.backend.predicates import expectation_plan

    contract = OpenDataContractStandard(
        version="0.1.0",
        kind="DataContract",
        apiVersion="3.0.2",
        id="test.float_format_extract",
        name="Float Format Extract",
        schema=[
            SchemaObject(
                name="values",
                properties=[
                    SchemaProperty(
                        name="my_value",
                        physicalType="string",
                        logicalType="float",
                        logicalTypeOptions={"decimalSeparator": ".", "thousandsSeparator": " "}
                    )
                ]
            )
        ]
    )

    plan = expectation_plan(contract)
    float_rules = [item for item in plan if item.get("rule") == "float_format"]
    assert len(float_rules) == 1
    assert float_rules[0]["column"] == "my_value"
    assert float_rules[0]["params"] == {"decimalSeparator": ".", "thousandsSeparator": " "}
    assert "predicate" in float_rules[0]


def test_sql_predicate_integer_format() -> None:
    # 1. No thousands separator (standard integer format)
    spec1 = ExpectationSpec(
        key="integer_format_val1",
        rule="integer_format",
        column="val1",
    )
    predicate1 = sql_predicate(spec1)
    assert predicate1 == r"`val1` IS NULL OR `val1` RLIKE '^[+-]?\\d+$'"

    # 2. Custom thousands separator (comma)
    spec2 = ExpectationSpec(
        key="integer_format_val2",
        rule="integer_format",
        column="val2",
        params={"thousandsSeparator": ","},
    )
    predicate2 = sql_predicate(spec2)
    assert predicate2 == r"`val2` IS NULL OR `val2` RLIKE '^[+-]?(?:\\d{1,3}(?:,\\d{3})+|\\d+)$'"

    # 3. Custom thousands separator (space)
    spec3 = ExpectationSpec(
        key="integer_format_val3",
        rule="integer_format",
        column="val3",
        params={"thousandsSeparator": " "},
    )
    predicate3 = sql_predicate(spec3)
    assert predicate3 == r"`val3` IS NULL OR `val3` RLIKE '^[+-]?(?:\\d{1,3}(?:\\ \\d{3})+|\\d+)$'"




def test_expectation_plan_extracts_integer_format() -> None:
    from open_data_contract_standard.model import (
        OpenDataContractStandard,
        SchemaObject,
        SchemaProperty,
    )
    from dc43_service_backends.data_quality.backend.predicates import expectation_plan

    contract = OpenDataContractStandard(
        version="0.1.0",
        kind="DataContract",
        apiVersion="3.0.2",
        id="test.integer_format_extract",
        name="Integer Format Extract",
        schema=[
            SchemaObject(
                name="values",
                properties=[
                    SchemaProperty(
                        name="my_value",
                        physicalType="string",
                        logicalType="integer",
                        logicalTypeOptions={"thousandsSeparator": " "}
                    )
                ]
            )
        ]
    )

    plan = expectation_plan(contract)
    int_rules = [item for item in plan if item.get("rule") == "integer_format"]
    assert len(int_rules) == 1
    assert int_rules[0]["column"] == "my_value"
    assert int_rules[0]["params"] == {"thousandsSeparator": " "}
    assert "predicate" in int_rules[0]





