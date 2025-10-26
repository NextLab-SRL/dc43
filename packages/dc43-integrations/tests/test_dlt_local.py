"""End-to-end tests for the local DLT harness."""

from __future__ import annotations

from datetime import datetime

import pytest

from dc43_integrations.spark.dlt import governed_table
from dc43_integrations.spark.dlt_local import LocalDLTHarness, ensure_dlt_module
from dc43_service_backends.contracts.backend.stores import FSContractStore
from dc43_service_clients.governance.models import GovernanceReadContext, ResolvedReadPlan
from .helpers.orders import build_orders_contract


dlt = ensure_dlt_module(allow_stub=True)


@pytest.mark.usefixtures("spark")
def test_local_harness_runs_governed_table(spark, tmp_path):
    store = FSContractStore(str(tmp_path / "contracts"))
    contract = build_orders_contract(tmp_path / "data")
    store.put(contract)

    plan = [
        {"key": "amount_positive", "predicate": "amount > 0"},
        {"key": "currency_enum", "predicate": "currency IN ('EUR', 'USD')"},
    ]

    class _HarnessGovernanceService:
        def resolve_read_context(self, *, context: GovernanceReadContext) -> ResolvedReadPlan:
            return ResolvedReadPlan(
                contract=contract,
                contract_id=contract.id,
                contract_version=contract.version,
                dataset_id=context.dataset_id or contract.id,
                dataset_version=context.dataset_version or contract.version,
                dataset_format=context.dataset_format,
                input_binding=context.input_binding,
                pipeline_context=None,
                bump=context.bump,
                draft_on_violation=context.draft_on_violation,
            )

        def describe_expectations(self, *, contract_id: str, contract_version: str):
            assert contract_id == contract.id
            assert contract_version == contract.version
            return list(plan)

    governance_service = _HarnessGovernanceService()

    data = [
        (1, 101, datetime(2024, 1, 1, 10, 0, 0), 10.0, "EUR"),
        (2, 102, datetime(2024, 1, 2, 10, 0, 0), -5.0, "EUR"),
        (3, 103, datetime(2024, 1, 3, 10, 0, 0), 15.0, "GBP"),
    ]

    columns = ["order_id", "customer_id", "order_ts", "amount", "currency"]

    with LocalDLTHarness(spark) as harness:

        @governed_table(
            dlt,
            name="orders",
            context={
                "contract": {
                    "contract_id": contract.id,
                    "contract_version": contract.version,
                }
            },
            governance_service=governance_service,
        )
        def orders():
            return spark.createDataFrame(data, columns)

        result = harness.run_asset("orders")

    order_ids = {row.order_id for row in result.collect()}

    # Local harness inspects expectations but leaves the DataFrame unchanged.
    assert order_ids == {1, 2, 3}

    reports = harness.expectation_reports
    assert any(report.action == "drop" and report.failed_rows == 1 for report in reports)

    binding = getattr(orders, "__dc43_contract_binding__")
    assert binding.contract_id == contract.id
    assert binding.expectations.enforced
