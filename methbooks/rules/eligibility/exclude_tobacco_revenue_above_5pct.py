"""
Purpose: Exclude companies deriving 5% or more aggregate revenue from production, distribution, retail, supply, and licensing of tobacco-related products, under the Highly Restrictive business involvement screen.
Datapoints: tobacco_revenue_pct.
Thresholds: REVENUE_THRESHOLD = 0.05 (module constant).
Source: methbooks/data/markdown/MSCI_Selection_Indexes_Methodology_20251211.md section "Appendix II: Controversial Business Involvement Criteria" near line 694.
See also: methbooks/rules/eligibility/exclude_tobacco_producer.py (companion producer flag screen applied before this rule).
"""
from __future__ import annotations
import polars as pl

REVENUE_THRESHOLD = 0.05

def exclude_tobacco_revenue_above_5pct(df: pl.DataFrame) -> pl.DataFrame:
    out = df.filter(pl.col("tobacco_revenue_pct") < REVENUE_THRESHOLD)
    assert "tobacco_revenue_pct" in out.columns, f"tobacco_revenue_pct column missing after filter: {out.columns}"
    assert float(out["tobacco_revenue_pct"].max() or 0) < REVENUE_THRESHOLD, (
        f"row with tobacco_revenue_pct >= {REVENUE_THRESHOLD} survived: max={out['tobacco_revenue_pct'].max()}"
    )
    return out
