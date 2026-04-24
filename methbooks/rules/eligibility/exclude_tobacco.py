"""
Purpose: Exclude companies involved in manufacturing tobacco products to meet EU CTB/PAB tobacco exclusion requirement.
Datapoints: bisr_tobacco_manufacturing_flag.
Thresholds: none.
Source: methbooks/data/markdown/MSCI_EU_CTB_PAB_Overlay_Indexes_Methodology_20260211.md section "2.3 Eligible Universe" near line 279.
See also: methbooks/rules/event_handling/quarterly_controversies_bisr_deletion.py (re-applied at quarterly review).
"""
from __future__ import annotations
import polars as pl

def exclude_tobacco(df: pl.DataFrame) -> pl.DataFrame:
    out = df.filter(pl.col("bisr_tobacco_manufacturing_flag") == False)  # noqa: E712
    assert "bisr_tobacco_manufacturing_flag" in out.columns, f"bisr_tobacco_manufacturing_flag column missing: {out.columns}"
    assert not out["bisr_tobacco_manufacturing_flag"].any(), f"Tobacco manufacturer survived: count={out['bisr_tobacco_manufacturing_flag'].sum()}"
    return out
