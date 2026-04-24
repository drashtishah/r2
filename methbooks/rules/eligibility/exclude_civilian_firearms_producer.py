"""
Purpose: Exclude companies classified as Producer of firearms and small arms ammunition for civilian markets under the Highly Restrictive business involvement screen; producer status triggers exclusion regardless of revenue.
Datapoints: civilian_firearms_producer_flag.
Thresholds: none.
Source: methbooks/data/markdown/MSCI_Selection_Indexes_Methodology_20251211.md section "Appendix II: Controversial Business Involvement Criteria" near line 674.
See also: methbooks/rules/eligibility/exclude_civilian_firearms_revenue_above_5pct.py (companion revenue-based screen for civilian firearms).
"""
from __future__ import annotations
import polars as pl

def exclude_civilian_firearms_producer(df: pl.DataFrame) -> pl.DataFrame:
    out = df.filter(pl.col("civilian_firearms_producer_flag") == False)  # noqa: E712
    assert "civilian_firearms_producer_flag" in out.columns, f"civilian_firearms_producer_flag column missing after filter: {out.columns}"
    assert out["civilian_firearms_producer_flag"].sum() == 0, (
        f"rows with civilian_firearms_producer_flag=True survived: {out['civilian_firearms_producer_flag'].sum()}"
    )
    return out
