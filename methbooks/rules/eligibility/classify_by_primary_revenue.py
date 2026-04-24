"""
Purpose: Assign a company to the Sub-Industry whose definition most accurately reflects the activity generating more than 60% of total revenues.
Datapoints: primary_activity_revenue_pct.
Thresholds: PRIMARY_REVENUE_THRESHOLD = 0.6.
Source: methbooks/data/markdown/MSCI_Global_Industry_Classification_Standard_(GICS®)_Methodology_20250220.md section "Section 3.1: Classification by revenue and earnings" near line 791.
See also: methbooks/rules/eligibility/classify_by_majority_revenue_and_earnings.py.
"""
from __future__ import annotations

import polars as pl

PRIMARY_REVENUE_THRESHOLD = 0.6


def classify_by_primary_revenue(df: pl.DataFrame) -> pl.DataFrame:
    out = df
    assert "primary_activity_revenue_pct" in out.columns, (
        f"primary_activity_revenue_pct column missing: {out.columns}"
    )
    assert (out["primary_activity_revenue_pct"] >= 0).all() and (out["primary_activity_revenue_pct"] <= 1).all(), (
        "primary_activity_revenue_pct values out of [0, 1]"
    )
    pct_classified = out.filter(pl.col("primary_activity_revenue_pct") > PRIMARY_REVENUE_THRESHOLD)
    min_pct = float(pct_classified["primary_activity_revenue_pct"].min()) if pct_classified.height > 0 else 1.0
    assert pct_classified["gics_sub_industry_code"].null_count() == 0, (
        f"rows with primary_activity_revenue_pct > {PRIMARY_REVENUE_THRESHOLD} have null gics_sub_industry_code: min_pct={min_pct}"
    )
    return out
