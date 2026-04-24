"""
Purpose: Exclude companies deriving >= 1% revenue from thermal coal mining.
Datapoints: thermal_coal_mining_revenue_pct.
Thresholds: 1.0 (revenue pct).
Source: methbooks/data/markdown/MSCI_Climate_Action_Indexes_Methodology_20251211.md section "Appendix I: ESG Business Involvement Eligibility" near line 604.
See also: methbooks/rules/event_handling/quarterly_controversies_bisr_deletion.py (re-applied at quarterly review).
"""
from __future__ import annotations

import polars as pl

THERMAL_COAL_MINING_THRESHOLD = 1.0


def exclude_thermal_coal_mining(df: pl.DataFrame) -> pl.DataFrame:
    out = df.filter(pl.col("thermal_coal_mining_revenue_pct") < THERMAL_COAL_MINING_THRESHOLD)
    assert "thermal_coal_mining_revenue_pct" in out.columns, (
        f"thermal_coal_mining_revenue_pct column missing: {out.columns}"
    )
    assert (out["thermal_coal_mining_revenue_pct"] < THERMAL_COAL_MINING_THRESHOLD).all(), (
        f"rows with thermal_coal_mining_revenue_pct >= {THERMAL_COAL_MINING_THRESHOLD} survived"
    )
    return out
