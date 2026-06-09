"""Thin SQL layer over the Databricks SQL connector.

Inside a Databricks App the service principal credentials are injected as
environment variables; databricks.sdk.Config picks them up transparently
(OAuth M2M). Locally, any standard auth method works (PAT, profile, CLI).
"""
from __future__ import annotations

import pandas as pd
import streamlit as st


@st.cache_resource(show_spinner=False)
def _connection(warehouse_id: str):
    from databricks import sql
    from databricks.sdk.core import Config

    cfg = Config()  # resolves host + app service principal automatically
    return sql.connect(
        server_hostname=cfg.host.replace("https://", ""),
        http_path=f"/sql/1.0/warehouses/{warehouse_id}",
        credentials_provider=lambda: cfg.authenticate,
    )


@st.cache_data(ttl=300, show_spinner=False)
def run_query(warehouse_id: str, query: str) -> pd.DataFrame:
    """Execute a SQL query against the bound warehouse, cached for 5 minutes."""
    conn = _connection(warehouse_id)
    with conn.cursor() as cur:
        cur.execute(query)
        rows = cur.fetchall()
        cols = [c[0] for c in cur.description]
    return pd.DataFrame(rows, columns=cols)
