"""
Purpose: Exclude securities in the bottom quartile of climate risk management score
    within their GICS sector.
Datapoints: crm_weighted_avg_score, gics_sector.
Thresholds: quartile 1 (bottom 25% per sector).
Source: methbooks/data/markdown/MSCI_Climate_Action_Indexes_Methodology_20251211.md section "2.2.4 Climate Risk Management Eligibility" near line 194.
See also: methbooks/rules/scoring/score_climate_risk_management.py (CRM score used in scoring phase).
"""
from __future__ import annotations

import polars as pl


def exclude_bottom_quartile_climate_risk_management(df: pl.DataFrame) -> pl.DataFrame:
    # Rank ascending: lowest crm_weighted_avg_score = rank 1 = quartile 1 (excluded)
    ranked = (
        df.with_columns(
            pl.col("crm_weighted_avg_score")
            .rank(method="ordinal", descending=False)
            .over("gics_sector")
            .alias("_rank")
        )
        .with_columns(
            pl.col("gics_sector").count().over("gics_sector").alias("_n")
        )
        .with_columns(
            (((pl.col("_rank") - 1) * 4) // pl.col("_n") + 1).cast(pl.Int32).alias("_quartile")
        )
    )
    out = ranked.filter(pl.col("_quartile") > 1).drop(["_rank", "_n", "_quartile"])
    assert "crm_weighted_avg_score" in out.columns, (
        f"crm_weighted_avg_score column missing: {out.columns}"
    )
    survivors_in_q1 = ranked.filter(pl.col("_quartile") == 1).filter(
        pl.col("security_id").is_in(out["security_id"])
    )
    assert survivors_in_q1.height == 0, (
        f"rows with sector-relative CRM quartile 1 survived: {survivors_in_q1.height}"
    )
    return out
