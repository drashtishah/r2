"""
Purpose: Exclude companies with MSCI Environmental Controversy Score of 0 (Red Flag) or 1 (Orange Flag), covering ongoing severe environmental controversies, to meet EU CTB/PAB environmental harm exclusion requirement.
Datapoints: msci_environmental_controversy_score.
Thresholds: red_flag_score = 0, orange_flag_score = 1 (module constants).
Source: methbooks/data/markdown/MSCI_EU_CTB_PAB_Overlay_Indexes_Methodology_20260211.md section "2.3 Eligible Universe" near line 267.
See also: methbooks/rules/eligibility/exclude_red_flag_controversies.py (sibling exclusion for overall controversy score).
"""
from __future__ import annotations
import polars as pl
ORANGE_FLAG_SCORE = 1

def exclude_red_orange_flag_environmental_controversies(df: pl.DataFrame) -> pl.DataFrame:
    out = df.filter(pl.col("msci_environmental_controversy_score") > ORANGE_FLAG_SCORE)
    assert "msci_environmental_controversy_score" in out.columns, f"msci_environmental_controversy_score column missing: {out.columns}"
    assert (out["msci_environmental_controversy_score"] > 1).all(), f"Red/Orange Flag environmental controversy score survived: min={out['msci_environmental_controversy_score'].min()}"
    return out
