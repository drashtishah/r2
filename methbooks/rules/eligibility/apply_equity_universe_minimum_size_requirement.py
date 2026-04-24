"""
Purpose: Exclude companies whose full market capitalization falls below the Equity
Universe Minimum Size Requirement, derived as the full market cap of the DM Equity
Universe company at 99% cumulative ff-adjusted coverage.
Datapoints: full_market_cap_usd, market_type, is_existing_imi_constituent.
Thresholds: DM_EM_COVERAGE_TARGET_PCT=99, FM_COVERAGE_TARGET_PCT=99,
  EXAMPLE_FEB_2026_DM_EM_USD_MM=507, EXAMPLE_FEB_2026_FM_USD_MM=13.
Source: meth-pipeline/MSCI_Global_Investable_Market_Indexes_Methodology_20260331/2026-04-24T07-46-35Z/input/markdown.md section "2.2.3 Equity Universe Minimum Size Requirement" near line 983.
See also: methbooks/rules/eligibility/apply_equity_universe_minimum_float_adjusted_mcap_requirement.py (companion size screen).
"""
from __future__ import annotations

import polars as pl

DM_EM_COVERAGE_TARGET_PCT = 99
FM_COVERAGE_TARGET_PCT = 99
EXAMPLE_FEB_2026_DM_EM_USD_MM = 507
EXAMPLE_FEB_2026_FM_USD_MM = 13


def apply_equity_universe_minimum_size_requirement(df: pl.DataFrame) -> pl.DataFrame:
    assert "full_market_cap_usd" in df.columns, (
        f"full_market_cap_usd column missing: {df.columns}"
    )
    assert "equity_universe_min_size_usd" in df.columns, (
        f"equity_universe_min_size_usd column missing: {df.columns}"
    )
    # Existing IMI constituents are exempt; new entrants must meet minimum size.
    out = df.filter(
        pl.col("is_existing_imi_constituent")
        | (pl.col("full_market_cap_usd") >= pl.col("equity_universe_min_size_usd"))
    )
    assert "full_market_cap_usd" in out.columns, (
        f"full_market_cap_usd column missing after filter: {out.columns}"
    )
    failing_non_constituents = out.filter(
        (~pl.col("is_existing_imi_constituent"))
        & (pl.col("full_market_cap_usd") < pl.col("equity_universe_min_size_usd"))
    )
    assert failing_non_constituents.height == 0, (
        f"non-constituent with full_market_cap_usd below equity_universe_min_size_usd"
        f" survived: {failing_non_constituents.height} rows;"
        f" min full_market_cap_usd={failing_non_constituents['full_market_cap_usd'].min()}"
    )
    return out
