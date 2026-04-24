"""
Purpose: Exclude tobacco producers and companies deriving >= 5% revenue from tobacco.
Datapoints: tobacco_producer_flag, tobacco_revenue_pct.
Thresholds: 5.0 (revenue pct).
Source: methbooks/data/markdown/MSCI_Climate_Action_Indexes_Methodology_20251211.md section "Eligibility Criteria" near line 1.
See also: methbooks/rules/event_handling/quarterly_controversies_bisr_deletion.py (re-applied at quarterly review).
"""
from __future__ import annotations

import polars as pl

TOBACCO_REVENUE_THRESHOLD = 5.0


def exclude_tobacco(df: pl.DataFrame) -> pl.DataFrame:
    out = df.filter(
        ~(
            pl.col("tobacco_producer_flag")
            | (pl.col("tobacco_revenue_pct") >= TOBACCO_REVENUE_THRESHOLD)
        )
    )
    assert "tobacco_producer_flag" in out.columns, (
        f"tobacco_producer_flag column missing: {out.columns}"
    )
    assert out["tobacco_producer_flag"].sum() == 0, (
        f"tobacco producer rows survived: {out['tobacco_producer_flag'].sum()}"
    )
    assert (out["tobacco_revenue_pct"] < TOBACCO_REVENUE_THRESHOLD).all(), (
        f"rows with tobacco_revenue_pct >= {TOBACCO_REVENUE_THRESHOLD} survived"
    )
    return out
