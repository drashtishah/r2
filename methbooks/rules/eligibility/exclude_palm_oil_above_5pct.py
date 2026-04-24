"""
Purpose: Exclude companies deriving 5% or more revenue from the production of Palm Oil, with no assigned restrictiveness level.
Datapoints: palm_oil_revenue_pct.
Thresholds: REVENUE_THRESHOLD = 0.05 (module constant).
Source: methbooks/data/markdown/MSCI_Selection_Indexes_Methodology_20251211.md section "Appendix II: Controversial Business Involvement Criteria" near line 749.
See also: methbooks/rules/eligibility/exclude_arctic_oil_gas_above_5pct.py (companion 5% screen for arctic oil and gas production).
"""
from __future__ import annotations
import polars as pl

REVENUE_THRESHOLD = 0.05

def exclude_palm_oil_above_5pct(df: pl.DataFrame) -> pl.DataFrame:
    out = df.filter(pl.col("palm_oil_revenue_pct") < REVENUE_THRESHOLD)
    assert "palm_oil_revenue_pct" in out.columns, f"palm_oil_revenue_pct column missing after filter: {out.columns}"
    assert float(out["palm_oil_revenue_pct"].max() or 0) < REVENUE_THRESHOLD, (
        f"row with palm_oil_revenue_pct >= {REVENUE_THRESHOLD} survived: max={out['palm_oil_revenue_pct'].max()}"
    )
    return out
