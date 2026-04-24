"""
Purpose: Require IPOs to have traded at least three months before Index Review
implementation; large IPOs and large primary/secondary offerings are exempt.
Datapoints: months_since_first_trading, is_large_ipo_flag, full_market_cap_usd,
  ff_adjusted_mcap_usd, standard_index_interim_cutoff_usd.
Thresholds: MIN_MONTHS_TRADING=3,
  SMALL_IPO_OVERRIDE_FULL_MCAP_MULTIPLIER=1.8,
  SMALL_IPO_OVERRIDE_FF_MCAP_MULTIPLIER=0.9.
Source: meth-pipeline/MSCI_Global_Investable_Market_Indexes_Methodology_20260331/2026-04-24T07-46-35Z/input/markdown.md section "2.2.7 Minimum Length of Trading Requirement" near line 1217.
See also: methbooks/rules/event_handling/early_inclusion_non_index_constituents_and_ipos.py (large IPO fast-track).
"""
from __future__ import annotations

import polars as pl

MIN_MONTHS_TRADING = 3
SMALL_IPO_OVERRIDE_FULL_MCAP_MULTIPLIER = 1.8
SMALL_IPO_OVERRIDE_FF_MCAP_MULTIPLIER = 0.9


def apply_minimum_length_of_trading_requirement(df: pl.DataFrame) -> pl.DataFrame:
    assert "months_since_first_trading" in df.columns, (
        f"months_since_first_trading column missing: {df.columns}"
    )
    cutoff = pl.col("standard_index_interim_cutoff_usd")
    is_large_by_size = (
        pl.col("full_market_cap_usd") >= SMALL_IPO_OVERRIDE_FULL_MCAP_MULTIPLIER * cutoff
    ) & (
        pl.col("ff_adjusted_mcap_usd") >= SMALL_IPO_OVERRIDE_FF_MCAP_MULTIPLIER * cutoff
    )
    out = df.filter(
        (pl.col("months_since_first_trading") >= MIN_MONTHS_TRADING)
        | pl.col("is_large_ipo_flag")
        | is_large_by_size
    )
    bad = out.filter(
        (pl.col("months_since_first_trading") < MIN_MONTHS_TRADING)
        & (~pl.col("is_large_ipo_flag"))
        & (~is_large_by_size)
    )
    assert bad.height == 0, (
        f"non-large IPO with months_since_first_trading < {MIN_MONTHS_TRADING}"
        f" not meeting size override survived: {bad.height} rows;"
        f" min months_since_first_trading={bad['months_since_first_trading'].min()}"
    )
    return out
