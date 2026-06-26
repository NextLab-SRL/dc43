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
