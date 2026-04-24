"""
Purpose: Exclude companies deriving 10% or more revenue from ownership or operation of gambling-related business activities, under the Least Restrictive business involvement screen.
Datapoints: gambling_revenue_pct.
Thresholds: REVENUE_THRESHOLD = 0.1 (module constant).
Source: methbooks/data/markdown/MSCI_Selection_Indexes_Methodology_20251211.md section "Appendix II: Controversial Business Involvement Criteria" near line 712.
See also: methbooks/rules/eligibility/exclude_alcohol_above_10pct.py (same 10% threshold, different CBI category).
"""
from __future__ import annotations
import polars as pl

REVENUE_THRESHOLD = 0.1

def exclude_gambling_above_10pct(df: pl.DataFrame) -> pl.DataFrame:
    out = df.filter(pl.col("gambling_revenue_pct") < REVENUE_THRESHOLD)
    assert "gambling_revenue_pct" in out.columns, f"gambling_revenue_pct column missing after filter: {out.columns}"
    assert float(out["gambling_revenue_pct"].max() or 0) < REVENUE_THRESHOLD, (
        f"row with gambling_revenue_pct >= {REVENUE_THRESHOLD} survived: max={out['gambling_revenue_pct'].max()}"
    )
    return out
