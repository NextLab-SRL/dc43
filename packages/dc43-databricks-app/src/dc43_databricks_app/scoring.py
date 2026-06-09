"""Criticality-aware scoring.

Score per table on a 0-100 scale, blending three signals:

  blast radius   (40%) -> distinct downstream tables fed (lineage)
  usage          (30%) -> read events over the lookback window
  declared       (30%) -> the `criticality` UC tag set by governance

The point of the blend: declared criticality is what governance *believes*,
lineage and usage are what the platform *observes*. Divergence between the two
is itself a governance finding (e.g. a "low" tagged table feeding 40 others).
"""
from __future__ import annotations

import numpy as np
import pandas as pd

_DECLARED = {"critical": 100, "high": 75, "medium": 50, "low": 25}


def _scale(series: pd.Series) -> pd.Series:
    """Log-scale to 0-100 so one mega-table doesn't flatten everything else."""
    s = np.log1p(series.fillna(0).astype(float))
    return 100 * s / s.max() if s.max() > 0 else s


def score_tables(
    activity: pd.DataFrame,        # table_fqn, read_events_*, distinct_consumers
    downstream: pd.DataFrame,      # table_fqn, downstream_tables
    tags: pd.DataFrame,            # table_fqn, declared_criticality
    freshness: pd.DataFrame,       # table_fqn, hours_since_update
) -> pd.DataFrame:
    read_col = next((c for c in activity.columns if c.startswith("read_events")), None)

    df = activity.merge(downstream, on="table_fqn", how="outer")
    df = df.merge(tags, on="table_fqn", how="left")
    df = df.merge(freshness[["table_fqn", "hours_since_update"]], on="table_fqn", how="left")

    df["usage_score"] = _scale(df[read_col]) if read_col else 0.0
    df["blast_score"] = _scale(df["downstream_tables"])
    df["declared_score"] = (
        df["declared_criticality"].str.lower().map(_DECLARED).fillna(0)
    )

    df["criticality_score"] = (
        0.40 * df["blast_score"]
        + 0.30 * df["usage_score"]
        + 0.30 * df["declared_score"]
    ).round(1)

    # Governance finding: observed importance far above declared importance.
    # Both sides normalised to 0-100 so a "critical"-tagged table is never flagged.
    observed = (0.57 * df["blast_score"] + 0.43 * df["usage_score"]).clip(0, 100)
    df["observed_score"] = observed.round(1)
    df["under_declared"] = (observed - df["declared_score"]) > 30

    return df.sort_values("criticality_score", ascending=False).reset_index(drop=True)


def risk_band(score: float) -> str:
    if score >= 70:
        return "critical"
    if score >= 45:
        return "high"
    if score >= 20:
        return "medium"
    return "low"
