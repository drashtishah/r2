"""
Purpose: Exclude companies not assessed by MSCI Solutions on Controversies, Climate Change Metrics, or BISR, ensuring all constituents have complete ESG and climate data coverage.
Datapoints: is_rated_msci_controversies, is_rated_msci_climate, is_rated_msci_bisr.
Thresholds: none.
Source: methbooks/data/markdown/MSCI_EU_CTB_PAB_Overlay_Indexes_Methodology_20260211.md section "2.7 Treatment of Unrated Companies" near line 490.
See also: methbooks/rules/eligibility/exclude_controversial_weapons_involvement.py (companion eligibility exclusion).
"""
from __future__ import annotations
import polars as pl

def exclude_unrated_msci_solutions(df: pl.DataFrame) -> pl.DataFrame:
    out = df.filter(
        pl.col("is_rated_msci_controversies")
        & pl.col("is_rated_msci_climate")
        & pl.col("is_rated_msci_bisr")
    )
    assert "is_rated_msci_controversies" in out.columns, f"is_rated_msci_controversies column missing: {out.columns}"
    assert "is_rated_msci_climate" in out.columns, f"is_rated_msci_climate column missing: {out.columns}"
    assert "is_rated_msci_bisr" in out.columns, f"is_rated_msci_bisr column missing: {out.columns}"
    assert out["is_rated_msci_controversies"].all(), f"Unrated Controversies company survived: count={int((~out['is_rated_msci_controversies']).sum())}"
    assert out["is_rated_msci_climate"].all(), f"Unrated Climate company survived: count={int((~out['is_rated_msci_climate']).sum())}"
    assert out["is_rated_msci_bisr"].all(), f"Unrated BISR company survived: count={int((~out['is_rated_msci_bisr']).sum())}"
    return out
