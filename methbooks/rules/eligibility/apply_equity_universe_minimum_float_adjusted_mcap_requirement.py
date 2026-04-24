"""
Purpose: Exclude individual securities whose ff-adjusted market cap is below 50% of
the Equity Universe Minimum Size Requirement to ensure minimum investable float.
Datapoints: ff_adjusted_mcap_usd, equity_universe_min_size_usd, is_existing_imi_constituent.
Thresholds: FF_MCAP_PCT_OF_MIN_SIZE=50.
Source: meth-pipeline/MSCI_Global_Investable_Market_Indexes_Methodology_20260331/2026-04-24T07-46-35Z/input/markdown.md section "2.2.4 Equity Universe Minimum Float-Adjusted Market Capitalization Requirement" near line 1033.
See also: methbooks/rules/eligibility/apply_equity_universe_minimum_size_requirement.py (companion screen on full mcap).
"""
from __future__ import annotations

import polars as pl

FF_MCAP_PCT_OF_MIN_SIZE = 50


def apply_equity_universe_minimum_float_adjusted_mcap_requirement(
    df: pl.DataFrame,
) -> pl.DataFrame:
    threshold = FF_MCAP_PCT_OF_MIN_SIZE / 100.0
    out = df.filter(
        pl.col("is_existing_imi_constituent")
        | (
            pl.col("ff_adjusted_mcap_usd")
            >= threshold * pl.col("equity_universe_min_size_usd")
        )
    )
    assert "ff_adjusted_mcap_usd" in out.columns, (
        f"ff_adjusted_mcap_usd column missing: {out.columns}"
    )
    failing = out.filter(
        (~pl.col("is_existing_imi_constituent"))
        & (
            pl.col("ff_adjusted_mcap_usd")
            < threshold * pl.col("equity_universe_min_size_usd")
        )
    )
    assert failing.height == 0, (
        f"non-constituent with ff_adjusted_mcap_usd < 0.5 * equity_universe_min_size_usd"
        f" survived: {failing.height} rows;"
        f" min ff_adjusted_mcap_usd={failing['ff_adjusted_mcap_usd'].min()}"
    )
    return out
