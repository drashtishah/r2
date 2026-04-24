"""
Purpose: Exclude companies with MSCI Controversy Score of 0 (Red Flag), indicating an ongoing Very Severe controversy implicating the company directly, to meet EU CTB/PAB controversy exclusion requirement.
Datapoints: msci_controversy_score.
Thresholds: red_flag_score = 0 (module constant).
Source: methbooks/data/markdown/MSCI_EU_CTB_PAB_Overlay_Indexes_Methodology_20260211.md section "2.3 Eligible Universe" near line 263.
See also: methbooks/rules/event_handling/quarterly_controversies_bisr_deletion.py (re-applied at quarterly review).
"""
from __future__ import annotations
import polars as pl
RED_FLAG_SCORE = 0

def exclude_red_flag_controversies(df: pl.DataFrame) -> pl.DataFrame:
    out = df.filter(pl.col("msci_controversy_score") > RED_FLAG_SCORE)
    assert "msci_controversy_score" in out.columns, f"msci_controversy_score column missing: {out.columns}"
    assert (out["msci_controversy_score"] > 0).all(), f"Red Flag controversy score survived: min={out['msci_controversy_score'].min()}"
    return out
