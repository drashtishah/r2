"""
Purpose: Apply Frontier Market tiered liquidity requirements (very low/low/average)
based on each market's liquidity category; require 12-month ATVR >= 2.5%, 5%, or 15%
and 12-month frequency of trading >= 50%.
Datapoints: atvr_12m_pct, freq_of_trading_12m_pct, fm_liquidity_category,
  is_existing_imi_constituent, ff_adjusted_mcap_usd.
Thresholds: VERY_LOW_ATVR_12M_MIN_PCT=2.5, LOW_ATVR_12M_MIN_PCT=5,
  AVERAGE_ATVR_12M_MIN_PCT=15, ALL_CATEGORIES_FREQ_12M_MIN_PCT=50,
  EXISTING_CONSTITUENT_AVERAGE_ATVR_FLOOR_PCT=10,
  EXISTING_CONSTITUENT_LOW_ATVR_FLOOR_PCT=3.33,
  EXISTING_CONSTITUENT_VERY_LOW_ATVR_FLOOR_PCT=1,
  EXISTING_CONSTITUENT_FREQ_12M_MIN_PCT=10.
Source: meth-pipeline/MSCI_Global_Investable_Market_Indexes_Methodology_20260331/2026-04-24T07-46-35Z/input/markdown.md section "5.2.3 FM Minimum Liquidity Requirement" near line 3611.
See also: methbooks/rules/eligibility/apply_dm_em_minimum_liquidity_requirement.py (DM/EM liquidity screen).
"""
from __future__ import annotations

import polars as pl

VERY_LOW_ATVR_12M_MIN_PCT = 2.5
LOW_ATVR_12M_MIN_PCT = 5.0
AVERAGE_ATVR_12M_MIN_PCT = 15.0
ALL_CATEGORIES_FREQ_12M_MIN_PCT = 50.0
EXISTING_CONSTITUENT_AVERAGE_ATVR_FLOOR_PCT = 10.0
EXISTING_CONSTITUENT_LOW_ATVR_FLOOR_PCT = 3.33
EXISTING_CONSTITUENT_VERY_LOW_ATVR_FLOOR_PCT = 1.0
EXISTING_CONSTITUENT_FREQ_12M_MIN_PCT = 10.0

_VALID_FM_CATEGORIES = {"very_low", "low", "average"}


def apply_fm_minimum_liquidity_requirement(df: pl.DataFrame) -> pl.DataFrame:
    assert "fm_liquidity_category" in df.columns, (
        f"fm_liquidity_category column missing: {df.columns}"
    )
    is_fm = pl.col("market_type") == "FM"
    is_new = ~pl.col("is_existing_imi_constituent")
    is_existing = pl.col("is_existing_imi_constituent")

    def _atvr_threshold_new(category: str) -> float:
        return {
            "very_low": VERY_LOW_ATVR_12M_MIN_PCT,
            "low": LOW_ATVR_12M_MIN_PCT,
            "average": AVERAGE_ATVR_12M_MIN_PCT,
        }[category]

    def _atvr_threshold_existing(category: str) -> float:
        return {
            "very_low": EXISTING_CONSTITUENT_VERY_LOW_ATVR_FLOOR_PCT,
            "low": EXISTING_CONSTITUENT_LOW_ATVR_FLOOR_PCT,
            "average": EXISTING_CONSTITUENT_AVERAGE_ATVR_FLOOR_PCT,
        }[category]

    passes_new = is_fm & is_new
    passes_existing = is_fm & is_existing
    not_fm = ~is_fm

    # Build per-category pass conditions.
    new_pass_expr = pl.lit(False)
    existing_pass_expr = pl.lit(False)
    for cat in ("very_low", "low", "average"):
        in_cat = pl.col("fm_liquidity_category") == cat
        new_pass_expr = new_pass_expr | (
            in_cat
            & (pl.col("atvr_12m_pct") >= _atvr_threshold_new(cat))
            & (pl.col("freq_of_trading_12m_pct") >= ALL_CATEGORIES_FREQ_12M_MIN_PCT)
        )
        existing_pass_expr = existing_pass_expr | (
            in_cat
            & (pl.col("atvr_12m_pct") >= _atvr_threshold_existing(cat))
            & (
                pl.col("freq_of_trading_12m_pct")
                >= EXISTING_CONSTITUENT_FREQ_12M_MIN_PCT
            )
        )

    out = df.filter(
        not_fm
        | (passes_new & new_pass_expr)
        | (passes_existing & existing_pass_expr)
    )

    # Business assert: non-constituents pass category-specific ATVR threshold.
    bad_new_average = out.filter(
        is_fm & is_new
        & (pl.col("fm_liquidity_category") == "average")
        & (pl.col("atvr_12m_pct") < AVERAGE_ATVR_12M_MIN_PCT)
    )
    assert bad_new_average.height == 0, (
        f"non-constituent FM average-liquidity security with atvr_12m_pct "
        f"< {AVERAGE_ATVR_12M_MIN_PCT} survived: {bad_new_average.height} rows"
    )
    return out
