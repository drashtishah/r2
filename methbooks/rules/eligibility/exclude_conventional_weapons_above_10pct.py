"""
Purpose: Exclude companies deriving 10% or more revenue from the production of conventional weapons and components, under the Least Restrictive business involvement screen.
Datapoints: conventional_weapons_revenue_pct.
Thresholds: REVENUE_THRESHOLD = 0.1 (module constant).
Source: methbooks/data/markdown/MSCI_Selection_Indexes_Methodology_20251211.md section "Appendix II: Controversial Business Involvement Criteria" near line 707.
See also: methbooks/rules/eligibility/exclude_nuclear_weapons.py (companion screen for nuclear weapons involvement).
"""
from __future__ import annotations
import polars as pl

REVENUE_THRESHOLD = 0.1

def exclude_conventional_weapons_above_10pct(df: pl.DataFrame) -> pl.DataFrame:
    out = df.filter(pl.col("conventional_weapons_revenue_pct") < REVENUE_THRESHOLD)
    assert "conventional_weapons_revenue_pct" in out.columns, f"conventional_weapons_revenue_pct column missing after filter: {out.columns}"
    assert float(out["conventional_weapons_revenue_pct"].max() or 0) < REVENUE_THRESHOLD, (
        f"row with conventional_weapons_revenue_pct >= {REVENUE_THRESHOLD} survived: max={out['conventional_weapons_revenue_pct'].max()}"
    )
    return out
