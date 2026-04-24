"""
Purpose: Exclude companies deriving 10% or more revenue from the ownership or operation of nuclear power plants, under the Least Restrictive business involvement screen.
Datapoints: nuclear_power_revenue_pct.
Thresholds: REVENUE_THRESHOLD = 0.1 (module constant).
Source: methbooks/data/markdown/MSCI_Selection_Indexes_Methodology_20251211.md section "Appendix II: Controversial Business Involvement Criteria" near line 718.
See also: methbooks/rules/eligibility/exclude_nuclear_weapons.py (companion screen for nuclear weapons, applied earlier in the pipeline).
"""
from __future__ import annotations
import polars as pl

REVENUE_THRESHOLD = 0.1

def exclude_nuclear_power_above_10pct(df: pl.DataFrame) -> pl.DataFrame:
    out = df.filter(pl.col("nuclear_power_revenue_pct") < REVENUE_THRESHOLD)
    assert "nuclear_power_revenue_pct" in out.columns, f"nuclear_power_revenue_pct column missing after filter: {out.columns}"
    assert float(out["nuclear_power_revenue_pct"].max() or 0) < REVENUE_THRESHOLD, (
        f"row with nuclear_power_revenue_pct >= {REVENUE_THRESHOLD} survived: max={out['nuclear_power_revenue_pct'].max()}"
    )
    return out
