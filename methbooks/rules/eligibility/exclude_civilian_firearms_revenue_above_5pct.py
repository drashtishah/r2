"""
Purpose: Exclude companies deriving 5% or more aggregate revenue from production and distribution of firearms or small arms ammunition for civilian use, under the Highly Restrictive business involvement screen.
Datapoints: civilian_firearms_revenue_pct.
Thresholds: REVENUE_THRESHOLD = 0.05 (module constant).
Source: methbooks/data/markdown/MSCI_Selection_Indexes_Methodology_20251211.md section "Appendix II: Controversial Business Involvement Criteria" near line 678.
See also: methbooks/rules/eligibility/exclude_civilian_firearms_producer.py (companion producer flag screen applied before this rule).
"""
from __future__ import annotations
import polars as pl

REVENUE_THRESHOLD = 0.05

def exclude_civilian_firearms_revenue_above_5pct(df: pl.DataFrame) -> pl.DataFrame:
    out = df.filter(pl.col("civilian_firearms_revenue_pct") < REVENUE_THRESHOLD)
    assert "civilian_firearms_revenue_pct" in out.columns, f"civilian_firearms_revenue_pct column missing after filter: {out.columns}"
    assert float(out["civilian_firearms_revenue_pct"].max() or 0) < REVENUE_THRESHOLD, (
        f"row with civilian_firearms_revenue_pct >= {REVENUE_THRESHOLD} survived: max={out['civilian_firearms_revenue_pct'].max()}"
    )
    return out
