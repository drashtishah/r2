"""
Purpose: Exclude companies deriving 1% or more revenue from thermal coal mining sold to external parties, as required for PAB Overlay Indexes under EU 2020/1818.
Datapoints: bisr_thermal_coal_mining_revenue_pct.
Thresholds: thermal_coal_mining_revenue_pct_threshold = 1 (module constant).
Source: methbooks/data/markdown/MSCI_EU_CTB_PAB_Overlay_Indexes_Methodology_20260211.md section "2.3 Eligible Universe" near line 286.
See also: methbooks/rules/event_handling/quarterly_controversies_bisr_deletion.py (re-applied at quarterly review).
"""
from __future__ import annotations
import polars as pl
THERMAL_COAL_MINING_REVENUE_PCT_THRESHOLD = 1.0

def exclude_thermal_coal_mining(df: pl.DataFrame) -> pl.DataFrame:
    out = df.filter(pl.col("bisr_thermal_coal_mining_revenue_pct") < THERMAL_COAL_MINING_REVENUE_PCT_THRESHOLD)
    assert "bisr_thermal_coal_mining_revenue_pct" in out.columns, f"bisr_thermal_coal_mining_revenue_pct column missing: {out.columns}"
    assert (out["bisr_thermal_coal_mining_revenue_pct"] < 1.0).all(), f"Thermal coal mining revenue >= 1% survived: max={out['bisr_thermal_coal_mining_revenue_pct'].max()}"
    return out
