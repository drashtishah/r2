"""
Purpose: When no single activity exceeds the 60% revenue threshold, assign the Sub-Industry that accounts for at least 50% of both revenues and earnings.
Datapoints: primary_activity_revenue_pct, primary_activity_earnings_pct.
Thresholds: SECONDARY_THRESHOLD = 0.5.
Source: methbooks/data/markdown/MSCI_Global_Industry_Classification_Standard_(GICS®)_Methodology_20250220.md section "Section 3.1: Classification by revenue and earnings" near line 793.
See also: methbooks/rules/eligibility/classify_by_primary_revenue.py.
"""
from __future__ import annotations

import polars as pl

SECONDARY_THRESHOLD = 0.5


def classify_by_majority_revenue_and_earnings(df: pl.DataFrame) -> pl.DataFrame:
    out = df
    assert "primary_activity_revenue_pct" in out.columns, (
        f"primary_activity_revenue_pct column missing: {out.columns}"
    )
    assert "primary_activity_earnings_pct" in out.columns, (
        f"primary_activity_earnings_pct column missing: {out.columns}"
    )
    assert (out["primary_activity_revenue_pct"] >= 0).all() and (out["primary_activity_revenue_pct"] <= 1).all(), (
        "primary_activity_revenue_pct out of [0, 1]"
    )
    assert (out["primary_activity_earnings_pct"] >= 0).all() and (out["primary_activity_earnings_pct"] <= 1).all(), (
        "primary_activity_earnings_pct out of [0, 1]"
    )
    mask = out.filter(
        (pl.col("primary_activity_revenue_pct") < 0.6)
        & (pl.col("primary_activity_revenue_pct") >= SECONDARY_THRESHOLD)
        & (pl.col("primary_activity_earnings_pct") >= SECONDARY_THRESHOLD)
    )
    assert mask["gics_sub_industry_code"].null_count() == 0, (
        "rows meeting 50% revenue and earnings threshold have null gics_sub_industry_code"
    )
    return out
