"""dc43 Control Tower — the dc43 Databricks App.

Governance-aware observability for data products on Databricks:
ODCS contracts (served by dc43 service clients) x Unity Catalog x system tables.

Runs in three modes with zero code change:
  - Databricks App with a bound SQL warehouse  -> live system tables
  - Local with Databricks auth configured      -> live system tables
  - Local with nothing configured              -> bundled demo data
"""
from __future__ import annotations

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

from dc43_databricks_app import data
from dc43_databricks_app.config import load_settings
from dc43_databricks_app.contracts_source import build_contract_client, load_latest_contracts
from dc43_databricks_app.drift import check_drift
from dc43_databricks_app.scoring import risk_band, score_tables

# ----------------------------------------------------------------- page setup

st.set_page_config(
    page_title="dc43 Control Tower",
    page_icon="🗼",
    layout="wide",
)

st.markdown(
    """
    <style>
      .block-container { padding-top: 2.2rem; max-width: 1280px; }
      [data-testid="stMetricValue"] { font-variant-numeric: tabular-nums; }
      code, .fqn { font-family: ui-monospace, "SF Mono", Menlo, monospace; }
      .pill {
        display:inline-block; padding:2px 10px; border-radius:999px;
        font-size:0.78rem; font-weight:600; letter-spacing:.02em;
      }
      .pill-critical { background:#3d1216; color:#ff6b6b; border:1px solid #ff6b6b44; }
      .pill-high     { background:#3a2a0e; color:#f5a623; border:1px solid #f5a62344; }
      .pill-medium   { background:#11303a; color:#4cc9f0; border:1px solid #4cc9f044; }
      .pill-low      { background:#11261b; color:#52d273; border:1px solid #52d27344; }
      .tower-title { font-size:1.9rem; font-weight:700; margin-bottom:0; }
      .tower-sub   { color:#8b93a7; margin-top:0.1rem; }
    </style>
    """,
    unsafe_allow_html=True,
)

SETTINGS = load_settings()
PALETTE = ["#F5A623", "#4CC9F0", "#52D273", "#FF6B6B", "#B89AF0", "#8B93A7"]


def pill(band: str) -> str:
    return f'<span class="pill pill-{band}">{band.upper()}</span>'


def chart_layout(fig: go.Figure) -> go.Figure:
    fig.update_layout(
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        font_color="#E6E8EE",
        margin=dict(l=10, r=10, t=30, b=10),
        colorway=PALETTE,
    )
    fig.update_xaxes(gridcolor="#222838")
    fig.update_yaxes(gridcolor="#222838")
    return fig


# ----------------------------------------------------------------- data loads

@st.cache_resource(show_spinner=False)
def _contract_client():
    return build_contract_client(SETTINGS)


try:
    contracts = load_latest_contracts(_contract_client())
    contract_source_error = None
except Exception as exc:  # remote backend unreachable, bad path...
    contracts, contract_source_error = [], str(exc)[:300]

activity = data.table_activity(SETTINGS)
downstream = data.downstream_consumers(SETTINGS)
tags = data.criticality_tags(SETTINGS)
freshness = data.table_freshness(SETTINGS)
scored = score_tables(activity, downstream, tags, freshness)
scored["band"] = scored["criticality_score"].apply(risk_band)

cost_daily = data.cost_daily(SETTINGS)
cost_workloads = data.cost_by_workload(SETTINGS)
jobs = data.job_health(SETTINGS)

contracted_fqns = {c.table_fqn for c in contracts if c.table_fqn}
scored["under_contract"] = scored["table_fqn"].isin(contracted_fqns)

# --------------------------------------------------------------------- header

left, right = st.columns([3, 1])
with left:
    st.markdown('<p class="tower-title">🗼 dc43 Control Tower</p>', unsafe_allow_html=True)
    st.markdown(
        '<p class="tower-sub">Data contracts × Unity Catalog × system tables — '
        "governance you can observe.</p>",
        unsafe_allow_html=True,
    )
with right:
    mode = "🟢 Live workspace" if SETTINGS.live else "🟡 Demo data"
    st.markdown(f"**Mode** {mode}")
    st.caption(f"Contracts: `{SETTINGS.contract_source_label}`")

if contract_source_error:
    st.error(f"Contract service unreachable: {contract_source_error}")

for w in st.session_state.get("data_warnings", set()):
    st.warning(f"Live query failed, showing demo data for that panel: {w}")

tab_overview, tab_contracts, tab_criticality, tab_cost = st.tabs(
    ["Overview", "Contracts & drift", "Criticality", "Cost & pipelines"]
)

# ------------------------------------------------------------------- overview

with tab_overview:
    cost_col = next(c for c in cost_workloads.columns if c.startswith("cost_usd"))
    total_cost = float(cost_workloads[cost_col].sum())
    critical_count = int((scored["band"] == "critical").sum())
    under_declared = int(scored["under_declared"].sum())
    coverage = (
        100.0 * scored["under_contract"].sum() / len(scored) if len(scored) else 0.0
    )

    m1, m2, m3, m4, m5 = st.columns(5)
    m1.metric("Active contracts", len(contracts))
    m2.metric("Contract coverage", f"{coverage:.0f}%", help="Share of observed tables bound to an ODCS contract")
    m3.metric("Critical assets", critical_count)
    m4.metric("Under-declared assets", under_declared,
              help="Tables whose observed importance (lineage + usage) far exceeds their declared criticality tag")
    m5.metric(f"Spend ({SETTINGS.lookback_days}d)", f"${total_cost:,.0f}")

    c1, c2 = st.columns([3, 2])
    with c1:
        st.subheader("Daily spend")
        fig = px.area(cost_daily, x="usage_date", y="cost_usd")
        fig.update_traces(line_color="#F5A623", fillcolor="rgba(245,166,35,0.15)")
        st.plotly_chart(chart_layout(fig), use_container_width=True)
    with c2:
        st.subheader("Risk posture")
        band_counts = scored["band"].value_counts().reindex(
            ["low", "medium", "high", "critical"], fill_value=0
        )
        fig = px.bar(
            band_counts, orientation="h",
            color=band_counts.index,
            color_discrete_map={
                "critical": "#FF6B6B", "high": "#F5A623",
                "medium": "#4CC9F0", "low": "#52D273",
            },
        )
        fig.update_layout(showlegend=False, yaxis_title=None, xaxis_title="tables")
        st.plotly_chart(chart_layout(fig), use_container_width=True)

    st.subheader("Needs attention")
    attention = scored[
        (scored["under_declared"]) | ((scored["band"].isin(["critical", "high"])) & (~scored["under_contract"]))
    ]
    if attention.empty:
        st.success("No governance gaps detected. Declared criticality matches observed usage.")
    else:
        for _, row in attention.head(6).iterrows():
            reason = []
            if row["under_declared"]:
                declared = row.get("declared_criticality") or "untagged"
                reason.append(f"observed importance far above declared tag (`{declared}`)")
            if not row["under_contract"]:
                reason.append("no data contract bound")
            st.markdown(
                f"{pill(row['band'])} &nbsp; <code>{row['table_fqn']}</code> — "
                f"score {row['criticality_score']:.0f}, {row['downstream_tables']:.0f} downstream tables. "
                f"{' · '.join(reason).capitalize()}.",
                unsafe_allow_html=True,
            )

# ------------------------------------------------------------------ contracts

with tab_contracts:
    if not contracts:
        st.info(
            "No contracts found. Set `DC43_CONTRACTS_URL` to a dc43 service backend, "
            "or `DC43_CONTRACT_PATH` to a UC volume in the FSContractStore layout: "
            "`<volume>/<contract_id>/<version>.json`."
        )
    else:
        summary = pd.DataFrame(
            [
                {
                    "contract": c.contract_id,
                    "version": c.version,
                    "status": c.status,
                    "bound table": c.table_fqn or "—",
                    "fields": len(c.properties),
                    "owner": c.owner,
                }
                for c in contracts
            ]
        )
        st.dataframe(summary, use_container_width=True, hide_index=True)

        st.divider()
        choice = st.selectbox(
            "Inspect a contract",
            options=[c.contract_id for c in contracts],
        )
        contract = next(c for c in contracts if c.contract_id == choice)

        c1, c2 = st.columns([2, 3])
        with c1:
            st.markdown(f"**{contract.name}** `v{contract.version}` — {contract.status}")
            st.caption(contract.description or "No description.")
            st.markdown("**Contracted schema**")
            st.dataframe(
                pd.DataFrame(
                    [
                        {
                            "field": p.get("name"),
                            "type": p.get("physicalType"),
                            "required": bool(p.get("required", False)),
                        }
                        for p in contract.properties
                    ]
                ),
                use_container_width=True,
                hide_index=True,
            )
        with c2:
            st.markdown("**Drift vs Unity Catalog**")
            if not contract.table_fqn:
                st.info("This contract has no bound UC table (`uc.table` custom property or server entry).")
            else:
                live_cols = data.table_columns(SETTINGS, contract.table_fqn)
                if live_cols.empty:
                    st.warning(f"Table `{contract.table_fqn}` not found in Unity Catalog.")
                else:
                    drift = check_drift(contract, live_cols)
                    breaking = (drift["severity"] == "breaking").sum()
                    warnings_n = (drift["severity"] == "warning").sum()
                    if breaking:
                        st.error(f"{breaking} breaking finding(s), {warnings_n} warning(s) — "
                                 "a new contract version should be drafted.")
                    elif warnings_n:
                        st.warning(f"{warnings_n} warning(s) — review recommended.")
                    else:
                        st.success("Contract and live table are in sync.")
                    icon = {"ok": "✅", "warning": "🟠", "breaking": "🔴"}
                    drift_view = drift.assign(severity=drift["severity"].map(icon) + " " + drift["severity"])
                    st.dataframe(drift_view, use_container_width=True, hide_index=True)

        with st.expander("Raw ODCS document"):
            st.json(contract.raw)

# ---------------------------------------------------------------- criticality

with tab_criticality:
    st.caption(
        "Score = 40% blast radius (downstream tables, lineage) + 30% observed usage "
        "(read events) + 30% declared criticality (UC tag). Divergence between observed "
        "and declared is flagged as a governance gap."
    )
    read_col = next((c for c in scored.columns if c.startswith("read_events")), None)
    view = scored[
        ["table_fqn", "criticality_score", "band", "downstream_tables",
         read_col, "declared_criticality", "under_contract", "under_declared",
         "hours_since_update"]
    ].rename(columns={
        "table_fqn": "table",
        "criticality_score": "score",
        read_col: "reads (30d)",
        "declared_criticality": "declared tag",
        "under_contract": "contracted",
        "under_declared": "gap",
        "hours_since_update": "stale (h)",
    })
    st.dataframe(
        view,
        use_container_width=True,
        hide_index=True,
        column_config={
            "score": st.column_config.ProgressColumn("score", min_value=0, max_value=100, format="%.0f"),
            "contracted": st.column_config.CheckboxColumn(),
            "gap": st.column_config.CheckboxColumn(help="Observed importance exceeds declared tag"),
        },
    )

    st.subheader("Observed vs declared")
    fig = px.scatter(
        scored,
        x="declared_score",
        y="observed_score",
        hover_name="table_fqn",
        color="band",
        color_discrete_map={
            "critical": "#FF6B6B", "high": "#F5A623",
            "medium": "#4CC9F0", "low": "#52D273",
        },
        labels={"x": "declared criticality", "y": "observed criticality"},
    )
    fig.add_shape(type="line", x0=0, y0=0, x1=100, y1=100,
                  line=dict(color="#8B93A7", dash="dot"))
    st.plotly_chart(chart_layout(fig), use_container_width=True)
    st.caption("Above the dotted line: the platform observes more importance than governance declared.")

# ----------------------------------------------------------------------- cost

with tab_cost:
    cost_col = next(c for c in cost_workloads.columns if c.startswith("cost_usd"))
    c1, c2 = st.columns(2)
    with c1:
        st.subheader(f"Top workloads by spend ({SETTINGS.lookback_days}d)")
        top = cost_workloads.nlargest(10, cost_col)
        fig = px.bar(
            top, x=cost_col, y="workload_id", orientation="h",
            color="workload_type",
        )
        fig.update_layout(yaxis=dict(autorange="reversed"), yaxis_title=None)
        st.plotly_chart(chart_layout(fig), use_container_width=True)
    with c2:
        st.subheader("Pipeline reliability")
        st.dataframe(
            jobs,
            use_container_width=True,
            hide_index=True,
            column_config={
                "success_rate_pct": st.column_config.ProgressColumn(
                    "success rate", min_value=0, max_value=100, format="%.1f%%"
                ),
            },
        )

    st.subheader("System table queries behind this page")
    st.caption("Transparency by design: every number above comes from a documented query on Databricks system tables.")
    from dc43_databricks_app import queries as q
    for label, sql in [
        ("Cost by workload (billing.usage × list_prices)", q.cost_by_workload(SETTINGS.lookback_days)),
        ("Table activity (access.table_lineage)", q.table_activity(SETTINGS.lookback_days)),
        ("Job health (lakeflow.job_run_timeline)", q.job_health(SETTINGS.lookback_days)),
    ]:
        with st.expander(label):
            st.code(sql, language="sql")
