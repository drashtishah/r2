"""
Purpose: Exclude companies deriving 5% or more aggregate revenue from arctic oil and gas production north of 66.5 degrees latitude, covering offshore and onshore production, with no assigned restrictiveness level.
Datapoints: arctic_oil_gas_revenue_pct.
Thresholds: REVENUE_THRESHOLD = 0.05 (module constant), ARCTIC_LATITUDE_THRESHOLD_DEGREES_NORTH = 66.5 (informational, not enforced in rule body as geography is pre-computed in the source datapoint).
Source: methbooks/data/markdown/MSCI_Selection_Indexes_Methodology_20251211.md section "Appendix II: Controversial Business Involvement Criteria" near line 753.
See also: methbooks/rules/eligibility/exclude_palm_oil_above_5pct.py (companion 5% screen for palm oil production).
"""
from __future__ import annotations
import polars as pl

REVENUE_THRESHOLD = 0.05
ARCTIC_LATITUDE_THRESHOLD_DEGREES_NORTH = 66.5

def exclude_arctic_oil_gas_above_5pct(df: pl.DataFrame) -> pl.DataFrame:
    out = df.filter(pl.col("arctic_oil_gas_revenue_pct") < REVENUE_THRESHOLD)
    assert "arctic_oil_gas_revenue_pct" in out.columns, f"arctic_oil_gas_revenue_pct column missing after filter: {out.columns}"
    assert float(out["arctic_oil_gas_revenue_pct"].max() or 0) < REVENUE_THRESHOLD, (
        f"row with arctic_oil_gas_revenue_pct >= {REVENUE_THRESHOLD} survived: max={out['arctic_oil_gas_revenue_pct'].max()}"
    )
    return out
