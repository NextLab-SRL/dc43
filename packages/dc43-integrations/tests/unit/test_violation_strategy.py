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
