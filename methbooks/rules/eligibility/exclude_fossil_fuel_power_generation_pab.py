"""
Purpose: Exclude companies deriving 50% or more aggregate revenue from thermal coal, liquid fuel, or natural gas based power generation, as required for PAB Overlay Indexes under EU 2020/1818.
Datapoints: bisr_fossil_fuel_power_generation_revenue_pct.
Thresholds: fossil_fuel_power_generation_revenue_pct_threshold = 50 (module constant).
Source: methbooks/data/markdown/MSCI_EU_CTB_PAB_Overlay_Indexes_Methodology_20260211.md section "2.3 Eligible Universe" near line 315.
See also: methbooks/rules/eligibility/exclude_oil_gas_activities_pab.py (PAB companion exclusion for oil and gas revenue).
"""
from __future__ import annotations
import polars as pl
FOSSIL_FUEL_POWER_GENERATION_REVENUE_PCT_THRESHOLD = 50.0

def exclude_fossil_fuel_power_generation_pab(df: pl.DataFrame) -> pl.DataFrame:
    out = df.filter(pl.col("bisr_fossil_fuel_power_generation_revenue_pct") < FOSSIL_FUEL_POWER_GENERATION_REVENUE_PCT_THRESHOLD)
    assert "bisr_fossil_fuel_power_generation_revenue_pct" in out.columns, f"bisr_fossil_fuel_power_generation_revenue_pct column missing: {out.columns}"
    assert (out["bisr_fossil_fuel_power_generation_revenue_pct"] < 50.0).all(), f"Fossil fuel power generation revenue >= 50% survived: max={out['bisr_fossil_fuel_power_generation_revenue_pct'].max()}"
    return out
