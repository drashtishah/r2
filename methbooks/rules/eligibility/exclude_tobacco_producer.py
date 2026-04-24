"""
Purpose: Exclude companies that manufacture tobacco products or grow or process raw tobacco leaves, under the Highly Restrictive business involvement screen.
Datapoints: tobacco_producer_flag.
Thresholds: none.
Source: methbooks/data/markdown/MSCI_Selection_Indexes_Methodology_20251211.md section "Appendix II: Controversial Business Involvement Criteria" near line 684.
See also: methbooks/rules/eligibility/exclude_tobacco_revenue_above_5pct.py (companion revenue-based screen for tobacco).
"""
from __future__ import annotations
import polars as pl

def exclude_tobacco_producer(df: pl.DataFrame) -> pl.DataFrame:
    out = df.filter(pl.col("tobacco_producer_flag") == False)  # noqa: E712
    assert "tobacco_producer_flag" in out.columns, f"tobacco_producer_flag column missing after filter: {out.columns}"
    assert out["tobacco_producer_flag"].sum() == 0, (
        f"rows with tobacco_producer_flag=True survived: {out['tobacco_producer_flag'].sum()}"
    )
    return out
