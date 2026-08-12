from __future__ import annotations

from dataclasses import dataclass
from typing import Callable, Iterable, Mapping, Optional

from dc43_service_clients.data_quality import ValidationResult
from types import SimpleNamespace

from dc43_integrations.spark.violation_strategy import (
    NoOpWriteViolationStrategy,
    SplitWriteViolationStrategy,
    StrictWriteViolationStrategy,
    WriteStrategyContext,
)


@dataclass
class FakeRow:
    valid: bool


class FakeDataFrame:
    def __init__(self, rows: Iterable[FakeRow]):
        self._rows = list(rows)

    def filter(self, predicate: str) -> "FakeDataFrame":
        predicate = predicate.strip()
        if predicate.startswith("NOT"):
            return FakeDataFrame(row for row in self._rows if not row.valid)
        return FakeDataFrame(row for row in self._rows if row.valid)

    def limit(self, count: int) -> "FakeDataFrame":
        return FakeDataFrame(self._rows[:count])

    def count(self) -> int:
        return len(self._rows)


class FakeValidation(ValidationResult):
    def __init__(
        self,
        warnings: Optional[list[str]] = None,
        metrics: Optional[dict[str, int]] = None,
    ) -> None:
        super().__init__(
            ok=False,
            warnings=warnings or ["violation"],
            metrics=metrics or {"violations.total": 1},
            status="warn",
        )


def make_context(
    *,
    rows: Iterable[FakeRow],
    revalidate: Optional[Callable[[FakeDataFrame], ValidationResult]] = None,
    expectation_predicates: Optional[Mapping[str, str]] = None,
) -> WriteStrategyContext:
    df = FakeDataFrame(rows)
    aligned = FakeDataFrame(rows)
    validation = FakeValidation()
    predicates = {"valid": "valid"} if expectation_predicates is None else expectation_predicates

    return WriteStrategyContext(
        df=df,
        aligned_df=aligned,
        contract=None,
        path="/tmp/dataset",
        table="analytics.orders",
        format="delta",
        options={"mergeSchema": "true"},
        mode="append",
        validation=validation,
        dataset_id="orders",
        dataset_version="v1",
        revalidate=revalidate or (lambda _: validation),
        expectation_predicates=predicates,
        pipeline_context=None,
    )


def test_noop_strategy_returns_base_request():
    context = make_context(rows=[FakeRow(valid=True)])
    plan = NoOpWriteViolationStrategy().plan(context)

    assert plan.primary is not None
    assert plan.primary.df is context.aligned_df
    assert plan.additional == ()


def test_split_strategy_creates_reject_request_when_invalid_rows_present():
    context = make_context(rows=[FakeRow(True), FakeRow(False)])
    strategy = SplitWriteViolationStrategy(write_primary_on_violation=False)
    plan = strategy.plan(context)

    assert plan.primary is None
    assert plan.additional
    reject = {req.dataset_id for req in plan.additional}
    assert "orders::reject" in reject


def test_split_strategy_returns_base_request_when_no_predicates():
    context = make_context(rows=[FakeRow(False)], expectation_predicates={})
    plan = SplitWriteViolationStrategy().plan(context)

    assert plan.primary is not None
    assert not plan.additional


def test_strict_strategy_inherits_contract_status_policy():
    base = NoOpWriteViolationStrategy(
        allowed_contract_statuses=("draft",),
        allow_missing_contract_status=False,
        contract_status_case_insensitive=False,
    )
    strict = StrictWriteViolationStrategy(base=base)
    contract = SimpleNamespace(id="orders", version="1.0.0", status="draft")

    # Using the decorator should honour the wrapped policy, so "draft" is accepted
    # instead of failing with the default "active" requirement.
    strict.validate_contract_status(contract=contract, enforce=True, operation="write")


def test_flag_strategy_returns_base_request_when_no_predicates():
    from dc43_integrations.spark.violation_strategy import FlagWriteViolationStrategy
    context = make_context(rows=[FakeRow(False)], expectation_predicates={})
    plan = FlagWriteViolationStrategy().plan(context)

    assert plan.primary is not None
    assert not plan.additional
    assert plan.primary.df is context.aligned_df


def test_flag_strategy_adds_corrupted_data_column(spark):
    from dc43_integrations.spark.violation_strategy import FlagWriteViolationStrategy
    import pyspark.sql.functions as F
    
    # We use a real dataframe to test the column addition
    df = spark.createDataFrame(
        [(1, 100), (2, -50), (3, 200)],
        ["id", "amount"]
    )
    
    validation = FakeValidation()
    context = WriteStrategyContext(
        df=df,
        aligned_df=df,
        contract=None,
        path="/tmp/dataset",
        table="analytics.orders",
        format="delta",
        options={},
        mode="append",
        validation=validation,
        dataset_id="orders",
        dataset_version="v1",
        revalidate=lambda _: validation,
        expectation_predicates={"amount_positive": "amount > 0", "id_positive": "id > 0"},
        pipeline_context=None,
    )
    
    strategy = FlagWriteViolationStrategy(column_name="_corrupted_data")
    plan = strategy.plan(context)
    
    assert plan.primary is not None
    assert plan.primary.df is not df
    assert "_corrupted_data" in plan.primary.df.columns
    
    results = plan.primary.df.orderBy("id").collect()
    # id 1: amount=100 -> valid (null)
    assert results[0]["_corrupted_data"] is None
    # id 2: amount=-50 -> invalid amount_positive
    assert results[1]["_corrupted_data"] == ["amount_positive"]
    # id 3: amount=200 -> valid (null)
    assert results[2]["_corrupted_data"] is None


def test_custom_deduplicate_quarantine_strategy(spark):
    from dataclasses import dataclass
    from typing import Literal, Sequence, Optional
    from pyspark.sql import Window
    from pyspark.sql import functions as F
    from dc43_integrations.spark.violation_strategy import (
        WriteViolationStrategy,
        WriteStrategyContext,
        WritePlan,
        WriteRequest,
    )

    @dataclass
    class DeduplicateQuarantineWriteViolationStrategy:
        key_properties: Sequence[str]
        sort_column: str
        keep: Literal["first", "last"] = "first"
        aggregate_quarantine: bool = False
        reject_suffix: str = "reject"

        def plan(self, context: WriteStrategyContext) -> WritePlan:
            df = context.aligned_df
            sort_order = F.col(self.sort_column).desc() if self.keep == "first" else F.col(self.sort_column).asc()
            window_spec = Window.partitionBy(*self.key_properties).orderBy(sort_order)
            df_ranked = df.withColumn("_row_num", F.row_number().over(window_spec))
            valid_df = df_ranked.filter(F.col("_row_num") == 1).drop("_row_num")
            reject_df = df_ranked.filter(F.col("_row_num") > 1).drop("_row_num")

            if self.aggregate_quarantine:
                other_cols = [c for c in df.columns if c not in self.key_properties]
                reject_df = reject_df.groupBy(*self.key_properties).agg(
                    F.collect_list(F.struct(*other_cols)).alias("quarantined_duplicates"),
                    F.count("*").alias("duplicate_count"),
                )

            primary_request = WriteRequest(
                df=valid_df,
                path=context.path,
                table=context.table,
                format=context.format,
                options=dict(context.options),
                mode=context.mode,
                contract=context.contract,
                dataset_id=context.dataset_id,
                dataset_version=context.dataset_version,
            )
            reject_request = WriteRequest(
                df=reject_df,
                path=f"{context.path}_{self.reject_suffix}" if context.path else None,
                table=f"{context.table}_{self.reject_suffix}" if context.table else None,
                format=context.format,
                options=dict(context.options),
                mode=context.mode,
                contract=context.contract,
                dataset_id=f"{context.dataset_id}::{self.reject_suffix}" if context.dataset_id else None,
                dataset_version=context.dataset_version,
            )
            return WritePlan(primary=primary_request, additional=[reject_request])

    # Sample data with duplicates for (tenant_id="t1", order_id="o1")
    df = spark.createDataFrame(
        [
            ("t1", "o1", 100, 10),
            ("t1", "o1", 150, 20),  # Newer updated_at (20)
            ("t1", "o2", 200, 15),
        ],
        ["tenant_id", "order_id", "amount", "updated_at"],
    )

    validation = FakeValidation()
    context = WriteStrategyContext(
        df=df,
        aligned_df=df,
        contract=None,
        path="/tmp/orders",
        table="analytics.orders",
        format="delta",
        options={},
        mode="append",
        validation=validation,
        dataset_id="orders",
        dataset_version="v1",
        revalidate=lambda _: validation,
        expectation_predicates={},
        pipeline_context=None,
    )

    # Test non-aggregated deduplication
    strat = DeduplicateQuarantineWriteViolationStrategy(
        key_properties=["tenant_id", "order_id"],
        sort_column="updated_at",
        keep="first",
        aggregate_quarantine=False,
    )
    plan = strat.plan(context)
    primary_rows = plan.primary.df.orderBy("order_id").collect()
    assert len(primary_rows) == 2
    # For (t1, o1), kept row is updated_at=20, amount=150
    assert primary_rows[0]["amount"] == 150
    assert primary_rows[0]["updated_at"] == 20

    reject_rows = plan.additional[0].df.collect()
    assert len(reject_rows) == 1
    assert reject_rows[0]["amount"] == 100

    # Test aggregated quarantine
    strat_agg = DeduplicateQuarantineWriteViolationStrategy(
        key_properties=["tenant_id", "order_id"],
        sort_column="updated_at",
        keep="first",
        aggregate_quarantine=True,
    )
    plan_agg = strat_agg.plan(context)
    agg_reject_rows = plan_agg.additional[0].df.collect()
    assert len(agg_reject_rows) == 1
    assert agg_reject_rows[0]["duplicate_count"] == 1
    assert len(agg_reject_rows[0]["quarantined_duplicates"]) == 1
    assert agg_reject_rows[0]["quarantined_duplicates"][0]["amount"] == 100


