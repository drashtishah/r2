"""
Purpose: Exclude companies deriving 5% or more aggregate revenue from thermal coal mining and unconventional oil and gas extraction combined, with no assigned restrictiveness level.
Datapoints: thermal_coal_mining_revenue_pct, unconventional_oil_gas_revenue_pct.
Thresholds: REVENUE_THRESHOLD = 0.05 (module constant).
Source: methbooks/data/markdown/MSCI_Selection_Indexes_Methodology_20251211.md section "Appendix II: Controversial Business Involvement Criteria" near line 727.
See also: methbooks/rules/eligibility/exclude_thermal_coal_power_above_5pct.py (companion screen for coal-based power generation).
"""
from __future__ import annotations
import polars as pl

REVENUE_THRESHOLD = 0.05

def exclude_fossil_fuel_extraction_above_5pct(df: pl.DataFrame) -> pl.DataFrame:
    out = df.filter(
        (pl.col("thermal_coal_mining_revenue_pct") + pl.col("unconventional_oil_gas_revenue_pct")) < REVENUE_THRESHOLD
    )
    assert "thermal_coal_mining_revenue_pct" in out.columns, (
        f"thermal_coal_mining_revenue_pct column missing after filter: {out.columns}"
    )
    assert "unconventional_oil_gas_revenue_pct" in out.columns, (
        f"unconventional_oil_gas_revenue_pct column missing after filter: {out.columns}"
    )
    combined_max = float(
        (out["thermal_coal_mining_revenue_pct"] + out["unconventional_oil_gas_revenue_pct"]).max() or 0
    )
    assert combined_max < REVENUE_THRESHOLD, (
        f"row with combined fossil fuel extraction revenue >= {REVENUE_THRESHOLD} survived: max={combined_max}"
    )
    return out
