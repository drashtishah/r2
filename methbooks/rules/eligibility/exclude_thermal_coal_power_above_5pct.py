"""
Purpose: Exclude companies deriving 5% or more revenue from thermal coal-based power generation, with no assigned restrictiveness level.
Datapoints: thermal_coal_power_revenue_pct.
Thresholds: REVENUE_THRESHOLD = 0.05 (module constant).
Source: methbooks/data/markdown/MSCI_Selection_Indexes_Methodology_20251211.md section "Appendix II: Controversial Business Involvement Criteria" near line 743.
See also: methbooks/rules/eligibility/exclude_fossil_fuel_extraction_above_5pct.py (companion screen for thermal coal mining).
"""
from __future__ import annotations
import polars as pl

REVENUE_THRESHOLD = 0.05

def exclude_thermal_coal_power_above_5pct(df: pl.DataFrame) -> pl.DataFrame:
    out = df.filter(pl.col("thermal_coal_power_revenue_pct") < REVENUE_THRESHOLD)
    assert "thermal_coal_power_revenue_pct" in out.columns, f"thermal_coal_power_revenue_pct column missing after filter: {out.columns}"
    assert float(out["thermal_coal_power_revenue_pct"].max() or 0) < REVENUE_THRESHOLD, (
        f"row with thermal_coal_power_revenue_pct >= {REVENUE_THRESHOLD} survived: max={out['thermal_coal_power_revenue_pct'].max()}"
    )
    return out
