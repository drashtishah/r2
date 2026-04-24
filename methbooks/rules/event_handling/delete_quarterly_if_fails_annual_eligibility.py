"""
Purpose: Delete existing Selection Index constituents at each Quarterly Index Review if they no longer meet the eligibility criteria defined in Section 3.1.1: ESG rating BB or above, Controversies Score 1 or above, and no triggered CBI screen flag.
Datapoints: msci_esg_rating, msci_controversies_score, is_current_constituent, cbi_screen_flag.
Thresholds: MIN_ESG_RATING_EXISTING = 'BB', MIN_CONTROVERSIES_SCORE_EXISTING = 1.
Source: methbooks/data/markdown/MSCI_Selection_Indexes_Methodology_20251211.md section "3.2 Quarterly Index Reviews" near line 418.
See also: methbooks/rules/event_handling/add_quarterly_if_eligible_and_sector_below_45pct.py (additions at the same Quarterly Index Review).
"""
from __future__ import annotations
import polars as pl

MIN_ESG_RATING_EXISTING = "BB"
MIN_CONTROVERSIES_SCORE_EXISTING = 1
_ELIGIBLE_RATINGS = {"AAA", "AA", "A", "BBB", "BB"}

def delete_quarterly_if_fails_annual_eligibility(df: pl.DataFrame) -> pl.DataFrame:
    out = df.filter(
        (~pl.col("is_current_constituent"))
        | (
            pl.col("msci_esg_rating").is_in(list(_ELIGIBLE_RATINGS))
            & (pl.col("msci_controversies_score") >= MIN_CONTROVERSIES_SCORE_EXISTING)
            & (~pl.col("cbi_screen_flag"))
        )
    )
    assert "cbi_screen_flag" in out.columns, f"cbi_screen_flag column missing: {out.columns}"
    surviving_low_esg = out.filter(
        pl.col("is_current_constituent") & (~pl.col("msci_esg_rating").is_in(list(_ELIGIBLE_RATINGS)))
    )
    assert surviving_low_esg.height == 0, (
        f"constituents with ESG rating below BB surviving quarterly deletion: {surviving_low_esg.height} rows, "
        f"ratings={surviving_low_esg['msci_esg_rating'].to_list()}"
    )
    surviving_low_score = out.filter(
        pl.col("is_current_constituent") & (pl.col("msci_controversies_score") < MIN_CONTROVERSIES_SCORE_EXISTING)
    )
    assert surviving_low_score.height == 0, (
        f"constituents with controversies score < {MIN_CONTROVERSIES_SCORE_EXISTING} surviving quarterly deletion: "
        f"{surviving_low_score.height} rows, scores={surviving_low_score['msci_controversies_score'].to_list()}"
    )
    surviving_cbi = out.filter(pl.col("is_current_constituent") & pl.col("cbi_screen_flag"))
    assert surviving_cbi.height == 0, (
        f"constituents with cbi_screen_flag=True surviving quarterly deletion: {surviving_cbi.height} rows"
    )
    return out
