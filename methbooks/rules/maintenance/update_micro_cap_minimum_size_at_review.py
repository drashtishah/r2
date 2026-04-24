"""
Purpose: Refresh the Micro Cap Minimum Size Requirement at each Index Review by
tracking DM Equity Universe coverage at the prior reference rank; reset when
coverage falls outside 99.7%-99.8% band.
Datapoints: cumulative_ff_mcap_coverage_pct, prior_micro_cap_reference_rank,
  full_market_cap_usd.
Thresholds: LOWER_BAND_PCT=99.7, UPPER_BAND_PCT=99.8.
Source: meth-pipeline/MSCI_Global_Investable_Market_Indexes_Methodology_20260331/2026-04-24T07-46-35Z/input/markdown.md section "4.2.1.1 Updating the Micro Cap Minimum Size Requirement" near line 3433.
See also: methbooks/rules/maintenance/update_equity_universe_minimum_size_at_review.py (parallel update for DM/EM universe minimum size).
"""
from __future__ import annotations

import polars as pl

LOWER_BAND_PCT = 99.7
UPPER_BAND_PCT = 99.8


def update_micro_cap_minimum_size_at_review(df: pl.DataFrame) -> pl.DataFrame:
    assert "cumulative_ff_mcap_coverage_pct" in df.columns, (
        f"cumulative_ff_mcap_coverage_pct column missing: {df.columns}"
    )
    assert "prior_micro_cap_reference_rank" in df.columns, (
        f"prior_micro_cap_reference_rank column missing: {df.columns}"
    )
    at_prior_rank = df.filter(pl.col("prior_micro_cap_reference_rank") == 1)
    if at_prior_rank.height > 0:
        coverage = float(at_prior_rank["cumulative_ff_mcap_coverage_pct"].item(0))
        in_band = LOWER_BAND_PCT <= coverage <= UPPER_BAND_PCT
        if not in_band:
            new_threshold = float(at_prior_rank["full_market_cap_usd"].item(0))
            out = df.with_columns(
                pl.lit(new_threshold).alias("micro_cap_min_size_usd")
            )
        else:
            out = df
    else:
        out = df
    assert "micro_cap_min_size_usd" in out.columns, (
        f"micro_cap_min_size_usd column missing after update: {out.columns}"
    )
    return out
