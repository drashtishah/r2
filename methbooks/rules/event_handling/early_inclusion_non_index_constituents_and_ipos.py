"""
Purpose: Allow immediate inclusion of non-index constituents and IPOs that are
significant in size and meet eligibility criteria (except length-of-trading and
liquidity), triggered by a corporate event or large IPO; included after close of
tenth day of trading for large IPOs.
Datapoints: full_market_cap_usd, ff_adjusted_mcap_usd, is_large_ipo_flag,
  days_since_ipo, interim_standard_cutoff_usd, interim_imi_cutoff_usd.
Thresholds: LARGE_IPO_EFFECTIVE_DATE_TRADING_DAYS=10,
  SPINOFF_STANDARD_FF_MCAP_MIN_PCT_OF_CUTOFF=50,
  SPINOFF_SMALL_CAP_FF_MCAP_MIN_PCT_OF_CUTOFF=50.
Source: meth-pipeline/MSCI_Global_Investable_Market_Indexes_Methodology_20260331/2026-04-24T07-46-35Z/input/markdown.md section "3.2.7 Early Inclusions of Non-Index Constituents and IPOs" near line 3166.
See also: methbooks/rules/event_handling/ipo_early_inclusion_standard_size_test.py (detailed size test from corporate_events methbook).
"""
from __future__ import annotations

import polars as pl

LARGE_IPO_EFFECTIVE_DATE_TRADING_DAYS = 10
SPINOFF_STANDARD_FF_MCAP_MIN_PCT_OF_CUTOFF = 50.0
SPINOFF_SMALL_CAP_FF_MCAP_MIN_PCT_OF_CUTOFF = 50.0


def early_inclusion_non_index_constituents_and_ipos(df: pl.DataFrame) -> pl.DataFrame:
    assert "full_market_cap_usd" in df.columns, (
        f"full_market_cap_usd column missing: {df.columns}"
    )
    assert "interim_standard_cutoff_usd" in df.columns, (
        f"interim_standard_cutoff_usd column missing: {df.columns}"
    )
    # Large IPOs: eligible after 10th trading day.
    large_ipo_eligible = pl.col("is_large_ipo_flag") & (
        pl.col("days_since_ipo") >= LARGE_IPO_EFFECTIVE_DATE_TRADING_DAYS
    )
    # Non-index constituents via corporate event: must meet minimum size.
    spinoff_eligible = (
        pl.col("ff_adjusted_mcap_usd")
        >= (SPINOFF_STANDARD_FF_MCAP_MIN_PCT_OF_CUTOFF / 100.0)
        * pl.col("interim_standard_cutoff_usd")
    )
    out = df.with_columns(
        (large_ipo_eligible | spinoff_eligible).alias("early_inclusion_eligible")
    )
    # Business assert: large IPOs included only after 10 trading days.
    premature_ipos = out.filter(
        pl.col("is_large_ipo_flag")
        & pl.col("early_inclusion_eligible")
        & (pl.col("days_since_ipo") < LARGE_IPO_EFFECTIVE_DATE_TRADING_DAYS)
    )
    assert premature_ipos.height == 0, (
        f"large IPO flagged early_inclusion_eligible before day"
        f" {LARGE_IPO_EFFECTIVE_DATE_TRADING_DAYS}: {premature_ipos.height} rows;"
        f" min days_since_ipo={premature_ipos['days_since_ipo'].min()}"
    )
    return out
