"""Single data access facade: live system tables when a warehouse is bound,
bundled demo data otherwise. Each accessor is fail-soft: a live query error
falls back to demo data and surfaces a warning instead of crashing the app.
"""
from __future__ import annotations

import pandas as pd
import streamlit as st

from . import demo_data, queries
from .config import Settings
from .warehouse import run_query


def _live_or_demo(settings: Settings, sql: str, demo_df: pd.DataFrame) -> pd.DataFrame:
    if not settings.live:
        return demo_df
    try:
        return run_query(settings.warehouse_id, sql)
    except Exception as exc:  # warehouse down, missing grants on system tables...
        st.session_state.setdefault("data_warnings", set()).add(str(exc)[:200])
        return demo_df


def cost_daily(s: Settings) -> pd.DataFrame:
    return _live_or_demo(s, queries.cost_daily(s.lookback_days), demo_data.cost_daily(s.lookback_days))


def cost_by_workload(s: Settings) -> pd.DataFrame:
    return _live_or_demo(
        s, queries.cost_by_workload(s.lookback_days), demo_data.cost_by_workload(s.lookback_days)
    )


def table_activity(s: Settings) -> pd.DataFrame:
    return _live_or_demo(
        s, queries.table_activity(s.lookback_days), demo_data.table_activity(s.lookback_days)
    )


def downstream_consumers(s: Settings) -> pd.DataFrame:
    return _live_or_demo(s, queries.downstream_consumers(), demo_data.downstream_consumers())


def criticality_tags(s: Settings) -> pd.DataFrame:
    return _live_or_demo(s, queries.criticality_tags(), demo_data.criticality_tags())


def table_freshness(s: Settings) -> pd.DataFrame:
    return _live_or_demo(s, queries.table_freshness(), demo_data.table_freshness())


def job_health(s: Settings) -> pd.DataFrame:
    return _live_or_demo(s, queries.job_health(s.lookback_days), demo_data.job_health(s.lookback_days))


def table_columns(s: Settings, table_fqn: str) -> pd.DataFrame:
    parts = table_fqn.split(".")
    if len(parts) != 3:
        return pd.DataFrame(columns=["column_name", "data_type", "is_nullable"])
    sql = queries.table_columns(*parts)
    return _live_or_demo(s, sql, demo_data.table_columns(table_fqn))
