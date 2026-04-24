"""
Purpose: Refresh the Equity Universe Minimum Size Requirement each Index Review by
checking cumulative ff-adjusted coverage at the prior reference rank; reset to 99%
or 99.25% coverage company when coverage drifts outside 99%-99.25% band.
Datapoints: cumulative_ff_mcap_coverage_pct, prior_reference_rank, full_market_cap_usd.
Thresholds: LOWER_BAND_PCT=99, UPPER_BAND_PCT=99.25.
Source: meth-pipeline/MSCI_Global_Investable_Market_Indexes_Methodology_20260331/2026-04-24T07-46-35Z/input/markdown.md section "3.1.2.2 Updating the Equity Universe Minimum Size Requirement" near line 2021.
See also: methbooks/rules/eligibility/apply_equity_universe_minimum_size_requirement.py (consumes equity_universe_min_size_usd).
"""
from __future__ import annotations

import polars as pl

LOWER_BAND_PCT = 99.0
UPPER_BAND_PCT = 99.25


def update_equity_universe_minimum_size_at_review(df: pl.DataFrame) -> pl.DataFrame:
    assert "cumulative_ff_mcap_coverage_pct" in df.columns, (
        f"cumulative_ff_mcap_coverage_pct column missing: {df.columns}"
    )
    assert "prior_reference_rank" in df.columns, (
        f"prior_reference_rank column missing: {df.columns}"
    )
    assert "full_market_cap_usd" in df.columns, (
        f"full_market_cap_usd column missing: {df.columns}"
    )
    # This rule updates the equity_universe_min_size_usd column based on coverage band.
    # When coverage at prior reference rank is within [99%, 99.25%], the threshold
    # is unchanged; when outside the band, reset to company at 99% or 99.25% coverage.
    at_prior_rank = df.filter(pl.col("prior_reference_rank") == 1)
    if at_prior_rank.height > 0:
        coverage = float(at_prior_rank["cumulative_ff_mcap_coverage_pct"].item(0))
        in_band = LOWER_BAND_PCT <= coverage <= UPPER_BAND_PCT
        # If within band, equity_universe_min_size_usd remains unchanged.
        if not in_band:
            # Reset: new threshold = full_market_cap_usd at this rank.
            new_threshold = float(at_prior_rank["full_market_cap_usd"].item(0))
            out = df.with_columns(
                pl.lit(new_threshold).alias("equity_universe_min_size_usd")
            )
        else:
            out = df
    else:
        out = df
    assert "equity_universe_min_size_usd" in out.columns, (
        f"equity_universe_min_size_usd column missing after update: {out.columns}"
    )
    return out
