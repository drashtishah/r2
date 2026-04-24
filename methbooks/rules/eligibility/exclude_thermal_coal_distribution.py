"""
Purpose: Exclude companies with evidence of thermal coal distribution or transport involvement, as required for PAB Overlay Indexes under EU 2020/1818.
Datapoints: bisr_thermal_coal_distribution_flag.
Thresholds: none.
Source: methbooks/data/markdown/MSCI_EU_CTB_PAB_Overlay_Indexes_Methodology_20260211.md section "2.3 Eligible Universe" near line 294.
See also: methbooks/rules/eligibility/exclude_thermal_coal_mining.py (sibling exclusion for thermal coal mining revenue).
"""
from __future__ import annotations
import polars as pl

def exclude_thermal_coal_distribution(df: pl.DataFrame) -> pl.DataFrame:
    out = df.filter(pl.col("bisr_thermal_coal_distribution_flag") == False)  # noqa: E712
    assert "bisr_thermal_coal_distribution_flag" in out.columns, f"bisr_thermal_coal_distribution_flag column missing: {out.columns}"
    assert not out["bisr_thermal_coal_distribution_flag"].any(), f"Thermal coal distribution involvement survived: count={out['bisr_thermal_coal_distribution_flag'].sum()}"
    return out
