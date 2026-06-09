"""Deterministic demo datasets shaped exactly like the system-table queries.

Used when no SQL warehouse is bound, so the app is fully demoable on a laptop
(streamlit run app.py) and degrades gracefully if warehouse calls fail.
"""
from __future__ import annotations

from datetime import datetime, timedelta

import numpy as np
import pandas as pd

_RNG = np.random.default_rng(43)
_NOW = datetime.utcnow()

TABLES = [
    ("main.sales.orders", "high", 4, 412, 38),
    ("main.sales.order_lines", "high", 2, 380, 31),
    ("main.crm.customers", "critical", 6, 295, 22),
    ("main.logistics.shipments", None, 3, 188, 17),       # untagged but heavily used
    ("main.logistics.carrier_rates", "medium", 5, 240, 12),
    ("main.finance.gl_postings", "critical", 12, 96, 9),
    ("main.marketing.campaign_touches", "low", 30, 64, 3),
    ("main.ops.device_telemetry", "low", 1, 22, 2),
    ("main.hr.headcount_snapshot", "medium", 168, 12, 4),
    ("main.staging.tmp_reconciliation", None, 720, 4, 0),
]


def cost_daily(days: int = 30) -> pd.DataFrame:
    dates = [(_NOW - timedelta(days=d)).date() for d in range(days, 0, -1)]
    base = 240 + 60 * np.sin(np.linspace(0, 3.5, days))
    noise = _RNG.normal(0, 18, days)
    spike = np.zeros(days)
    spike[-6] = 310  # a runaway job, makes the cost story visible
    return pd.DataFrame({"usage_date": dates, "cost_usd": (base + noise + spike).round(2)})


def cost_by_workload(days: int = 30) -> pd.DataFrame:
    rows = [
        ("job", "847211990", "PREMIUM_JOBS_COMPUTE", 2841.50, 1620.4),
        ("job", "112408335", "PREMIUM_JOBS_COMPUTE", 1932.10, 1101.0),
        ("warehouse", "9f2ce11ab3", "PREMIUM_SQL_PRO_COMPUTE", 1410.75, 705.2),
        ("job", "566120877", "PREMIUM_DLT_ADVANCED_COMPUTE", 1188.00, 540.1),
        ("warehouse", "1a77c02de9", "PREMIUM_SERVERLESS_SQL", 644.20, 322.0),
        ("job", "990341002", "PREMIUM_JOBS_COMPUTE", 96.40, 55.1),
        ("other", "other", "PREMIUM_ALL_PURPOSE_COMPUTE", 411.90, 230.7),
    ]
    return pd.DataFrame(
        rows, columns=["workload_type", "workload_id", "sku_name", f"cost_usd_{days}d", "dbus"]
    )


def table_activity(days: int = 30) -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "table_fqn": fqn,
                f"read_events_{days}d": reads,
                "distinct_consumers": max(1, reads // 18),
                "distinct_entities": max(1, reads // 25),
                "last_read_at": _NOW - timedelta(hours=int(_RNG.integers(1, 48))),
            }
            for fqn, _tag, _stale, reads, _down in TABLES
        ]
    )


def downstream_consumers() -> pd.DataFrame:
    return pd.DataFrame(
        [{"table_fqn": fqn, "downstream_tables": down} for fqn, _t, _s, _r, down in TABLES]
    )


def criticality_tags() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {"table_fqn": fqn, "declared_criticality": tag}
            for fqn, tag, _s, _r, _d in TABLES
            if tag
        ]
    )


def table_freshness() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "table_fqn": fqn,
                "last_altered": _NOW - timedelta(hours=stale),
                "hours_since_update": stale,
            }
            for fqn, _t, stale, _r, _d in TABLES
        ]
    )


def job_health(days: int = 30) -> pd.DataFrame:
    rows = [
        ("847211990", 124, 121, 97.6),
        ("112408335", 118, 118, 100.0),
        ("566120877", 60, 51, 85.0),
        ("990341002", 30, 30, 100.0),
    ]
    return pd.DataFrame(rows, columns=["job_id", "runs", "succeeded", "success_rate_pct"]).assign(
        last_run_at=_NOW - timedelta(hours=2)
    )


def table_columns(table_fqn: str) -> pd.DataFrame:
    """Live UC columns for demo drift checks (orders has deliberate drift)."""
    schemas = {
        "main.sales.orders": [
            ("order_id", "bigint", "NO"),
            ("customer_id", "bigint", "NO"),
            ("order_ts", "timestamp", "NO"),
            ("amount", "decimal(18,2)", "NO"),       # contract says double -> type drift
            ("currency", "string", "YES"),           # contract requires -> nullable warning
            ("sales_channel", "string", "YES"),      # new column, not in contract
        ],
        "main.logistics.shipments": [
            ("shipment_id", "bigint", "NO"),
            ("order_id", "bigint", "NO"),
            ("carrier_code", "string", "NO"),
            ("status", "string", "NO"),
            ("event_ts", "timestamp", "NO"),
            ("weight_kg", "double", "YES"),
        ],
    }
    cols = schemas.get(table_fqn, [("id", "bigint", "NO")])
    return pd.DataFrame(cols, columns=["column_name", "data_type", "is_nullable"])
