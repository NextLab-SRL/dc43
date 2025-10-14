"""End-to-end tests for the local DLT harness."""

from __future__ import annotations

from datetime import datetime

import pytest

from dc43_integrations.spark.dlt import contract_table
from dc43_integrations.spark.dlt_local import LocalDLTHarness, ensure_dlt_module
from dc43_service_backends.contracts.backend.stores import FSContractStore
from dc43_service_clients.contracts import LocalContractServiceClient
from dc43_service_clients.data_quality.client.local import LocalDataQualityServiceClient
from .helpers.orders import build_orders_contract


dlt = ensure_dlt_module(allow_stub=True)


@pytest.mark.usefixtures("spark")
def test_local_harness_runs_contract_table(spark, tmp_path):
    store = FSContractStore(str(tmp_path / "contracts"))
    contract = build_orders_contract(tmp_path / "data")
    store.put(contract)

    contract_service = LocalContractServiceClient(store)
    data_quality_service = LocalDataQualityServiceClient()

    data = [
        (1, 101, datetime(2024, 1, 1, 10, 0, 0), 10.0, "EUR"),
        (2, 102, datetime(2024, 1, 2, 10, 0, 0), -5.0, "EUR"),
        (3, 103, datetime(2024, 1, 3, 10, 0, 0), 15.0, "GBP"),
    ]

    columns = ["order_id", "customer_id", "order_ts", "amount", "currency"]

    with LocalDLTHarness(spark) as harness:

        @contract_table(
            dlt,
            name="orders",
            contract_id=contract.id,
            contract_service=contract_service,
            data_quality_service=data_quality_service,
            expected_contract_version="==0.1.0",
        )
        def orders():
            return spark.createDataFrame(data, columns)

        result = harness.run_asset("orders")

    order_ids = {row.order_id for row in result.collect()}

    # Invalid amount and currency rows should be removed.
    assert order_ids == {1}

    reports = harness.expectation_reports
    assert any(report.action == "drop" and report.failed_rows == 1 for report in reports)

    binding = getattr(orders, "__dc43_contract_binding__")
    assert binding.contract_id == contract.id
    assert binding.expectations.enforced
