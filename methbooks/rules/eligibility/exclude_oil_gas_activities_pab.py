"""
Purpose: Exclude companies deriving 10% or more aggregate revenue from oil and gas related activities, as required for PAB Overlay Indexes under EU 2020/1818.
Datapoints: bisr_oil_gas_revenue_pct.
Thresholds: oil_gas_revenue_pct_threshold = 10 (module constant).
Source: methbooks/data/markdown/MSCI_EU_CTB_PAB_Overlay_Indexes_Methodology_20260211.md section "2.3 Eligible Universe" near line 310.
See also: methbooks/rules/eligibility/exclude_thermal_coal_mining.py (PAB companion exclusion for thermal coal mining revenue).
"""
from __future__ import annotations
import polars as pl
OIL_GAS_REVENUE_PCT_THRESHOLD = 10.0

def exclude_oil_gas_activities_pab(df: pl.DataFrame) -> pl.DataFrame:
    out = df.filter(pl.col("bisr_oil_gas_revenue_pct") < OIL_GAS_REVENUE_PCT_THRESHOLD)
    assert "bisr_oil_gas_revenue_pct" in out.columns, f"bisr_oil_gas_revenue_pct column missing: {out.columns}"
    assert (out["bisr_oil_gas_revenue_pct"] < 10.0).all(), f"Oil & gas revenue >= 10% survived: max={out['bisr_oil_gas_revenue_pct'].max()}"
    return out
