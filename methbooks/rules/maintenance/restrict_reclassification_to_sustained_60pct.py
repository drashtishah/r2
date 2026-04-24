"""
Purpose: Limit GICS Sub-Industry changes to cases where a different business activity accounts for at least 60% of revenues over a sustained period, preventing unnecessary turnover.
Datapoints: primary_activity_revenue_pct, current_gics_sub_industry_code.
Thresholds: RECLASSIFICATION_REVENUE_THRESHOLD = 0.6.
Source: methbooks/data/markdown/MSCI_Global_Industry_Classification_Standard_(GICS®)_Methodology_20250220.md section "Section 3.1: Classification by revenue and earnings" near line 811.
See also: methbooks/rules/maintenance/minimize_reclassification_on_temporary_fluctuations.py.
"""
from __future__ import annotations

import polars as pl

RECLASSIFICATION_REVENUE_THRESHOLD = 0.6


def restrict_reclassification_to_sustained_60pct(df: pl.DataFrame) -> pl.DataFrame:
    out = df.with_columns(
        pl.when(
            (pl.col("current_gics_sub_industry_code") != pl.col("gics_sub_industry_code"))
            & (pl.col("primary_activity_revenue_pct") < RECLASSIFICATION_REVENUE_THRESHOLD)
        )
        .then(pl.col("current_gics_sub_industry_code"))
        .otherwise(pl.col("gics_sub_industry_code"))
        .alias("gics_sub_industry_code")
    )
    assert "current_gics_sub_industry_code" in out.columns, (
        f"current_gics_sub_industry_code column missing: {out.columns}"
    )
    assert "primary_activity_revenue_pct" in out.columns, (
        f"primary_activity_revenue_pct column missing: {out.columns}"
    )
    reclassified = out.filter(pl.col("current_gics_sub_industry_code") != pl.col("gics_sub_industry_code"))
    assert reclassified["primary_activity_revenue_pct"].min() is None or float(
        reclassified["primary_activity_revenue_pct"].min()
    ) >= RECLASSIFICATION_REVENUE_THRESHOLD, (
        f"reclassification applied where revenue_pct < {RECLASSIFICATION_REVENUE_THRESHOLD}: "
        f"offending pct={float(reclassified['primary_activity_revenue_pct'].min())}"
    )
    return out
