"""Tests for the dc43 Databricks App core logic (no Streamlit, no warehouse)."""
from __future__ import annotations

import pandas as pd
import pytest
from open_data_contract_standard.model import OpenDataContractStandard

from dc43_databricks_app.config import Settings
from dc43_databricks_app.contracts_source import (
    bound_table_fqn,
    build_contract_client,
    load_latest_contracts,
    summarize,
)
from dc43_databricks_app.drift import check_drift
from dc43_databricks_app.scoring import risk_band, score_tables


def _settings(**overrides) -> Settings:
    base = dict(
        warehouse_id=None,
        contracts_url=None,
        contracts_token=None,
        contract_path=None,
    )
    base.update(overrides)
    return Settings(**base)


@pytest.fixture()
def orders_contract() -> OpenDataContractStandard:
    return OpenDataContractStandard.model_validate(
        {
            "apiVersion": "3.0.2",
            "kind": "DataContract",
            "id": "sales.orders",
            "name": "Orders",
            "version": "0.1.0",
            "status": "active",
            "schema": [
                {
                    "name": "orders",
                    "physicalName": "orders",
                    "properties": [
                        {"name": "order_id", "physicalType": "bigint", "required": True},
                        {"name": "amount", "physicalType": "double", "required": True},
                        {"name": "currency", "physicalType": "string", "required": True},
                    ],
                }
            ],
            "customProperties": [{"property": "uc.table", "value": "main.sales.orders"}],
        }
    )


# ------------------------------------------------------------ contract source


def test_demo_contracts_resolve_through_local_client():
    client = build_contract_client(_settings())
    summaries = load_latest_contracts(client)
    ids = {s.contract_id for s in summaries}
    assert {"sales.orders", "logistics.shipments"} <= ids
    orders = next(s for s in summaries if s.contract_id == "sales.orders")
    assert orders.table_fqn == "main.sales.orders"
    assert orders.properties, "schema properties should be flattened for the UI"


def test_bound_table_falls_back_to_server_entry():
    contract = OpenDataContractStandard.model_validate(
        {
            "apiVersion": "3.0.2",
            "kind": "DataContract",
            "id": "x.y",
            "version": "1.0.0",
            "servers": [{"server": "prod", "type": "databricks", "catalog": "main", "schema": "sales"}],
            "schema": [{"name": "orders", "physicalName": "orders", "properties": []}],
        }
    )
    assert bound_table_fqn(contract) == "main.sales.orders"


# -------------------------------------------------------------------- drift


def test_drift_breaking_on_type_mismatch_and_missing_column(orders_contract):
    live = pd.DataFrame(
        [
            ("order_id", "bigint", "NO"),
            ("amount", "decimal(18,2)", "NO"),     # contract says double
            ("sales_channel", "string", "YES"),     # not in contract
        ],
        columns=["column_name", "data_type", "is_nullable"],
    )
    drift = check_drift(summarize(orders_contract), live)
    by_column = {(r.column, r.severity) for r in drift.itertuples()}
    assert ("amount", "breaking") in by_column          # type drift
    assert ("currency", "breaking") in by_column        # missing in live table
    assert ("sales_channel", "warning") in by_column    # uncontracted column
    assert ("order_id", "ok") in by_column


def test_drift_in_sync(orders_contract):
    live = pd.DataFrame(
        [
            ("order_id", "bigint", "NO"),
            ("amount", "double", "NO"),
            ("currency", "string", "NO"),
        ],
        columns=["column_name", "data_type", "is_nullable"],
    )
    drift = check_drift(summarize(orders_contract), live)
    assert set(drift["severity"]) == {"ok"}


# ------------------------------------------------------------------- scoring


def _frames():
    activity = pd.DataFrame(
        {
            "table_fqn": ["a.b.tagged_critical", "a.b.untagged_hub", "a.b.quiet"],
            "read_events_30d": [300, 280, 2],
            "distinct_consumers": [20, 18, 1],
            "distinct_entities": [15, 14, 1],
            "last_read_at": pd.Timestamp.utcnow(),
        }
    )
    downstream = pd.DataFrame(
        {"table_fqn": activity["table_fqn"], "downstream_tables": [25, 22, 0]}
    )
    tags = pd.DataFrame(
        {"table_fqn": ["a.b.tagged_critical"], "declared_criticality": ["critical"]}
    )
    freshness = pd.DataFrame(
        {"table_fqn": activity["table_fqn"], "hours_since_update": [2, 3, 500]}
    )
    return activity, downstream, tags, freshness


def test_under_declared_flags_only_the_governance_gap():
    scored = score_tables(*_frames())
    flags = dict(zip(scored["table_fqn"], scored["under_declared"]))
    assert flags["a.b.untagged_hub"] is True or flags["a.b.untagged_hub"] == True  # noqa: E712
    assert not flags["a.b.tagged_critical"], "a critical-tagged hub must not be flagged"
    assert not flags["a.b.quiet"]


def test_risk_bands_are_monotonic():
    assert risk_band(90) == "critical"
    assert risk_band(50) == "high"
    assert risk_band(30) == "medium"
    assert risk_band(5) == "low"
