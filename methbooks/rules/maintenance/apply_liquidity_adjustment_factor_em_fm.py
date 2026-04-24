"""
Purpose: Apply a 0.5 Liquidity Adjustment Factor to weight of EM Standard Index
constituents that fail liquidity but hold > 10% country weight and ff-adjusted
mcap > 0.5x Global Minimum Size Reference; delete or restore LAF at subsequent
reviews per criteria.
Datapoints: atvr_12m_pct, country_weight_pct, ff_adjusted_mcap_usd,
  global_min_size_reference_em_usd, market_type, liquidity_adjustment_factor,
  consecutive_reviews_meeting_new_constituent_liquidity.
Thresholds: COUNTRY_WEIGHT_THRESHOLD_PCT=10, FM_COMPOSITE_WEIGHT_THRESHOLD_PCT=1,
  FF_MCAP_PCT_OF_GMSR=50, LAF_VALUE=0.5, REVIEWS_TO_RESTORE_LAF=3.
Source: meth-pipeline/MSCI_Global_Investable_Market_Indexes_Methodology_20260331/2026-04-24T07-46-35Z/input/markdown.md section "3.1.2.4 Minimum Liquidity Requirement for Existing Constituents" near line 2087.
See also: methbooks/rules/eligibility/apply_dm_em_minimum_liquidity_requirement.py (primary liquidity screen).
"""
from __future__ import annotations

import polars as pl

COUNTRY_WEIGHT_THRESHOLD_PCT = 10.0
FM_COMPOSITE_WEIGHT_THRESHOLD_PCT = 1.0
FF_MCAP_PCT_OF_GMSR = 50.0
LAF_VALUE = 0.5
REVIEWS_TO_RESTORE_LAF = 3


def apply_liquidity_adjustment_factor_em_fm(df: pl.DataFrame) -> pl.DataFrame:
    assert "liquidity_adjustment_factor" in df.columns, (
        f"liquidity_adjustment_factor column missing: {df.columns}"
    )
    is_em = pl.col("market_type") == "EM"
    fails_liq = pl.col("atvr_12m_pct") < 15.0
    large_country_weight = pl.col("country_weight_pct") > COUNTRY_WEIGHT_THRESHOLD_PCT
    large_ff_mcap = (
        pl.col("ff_adjusted_mcap_usd")
        > (FF_MCAP_PCT_OF_GMSR / 100.0) * pl.col("global_min_size_reference_em_usd")
    )
    qualifies_for_laf = is_em & fails_liq & large_country_weight & large_ff_mcap
    restored = (
        pl.col("consecutive_reviews_meeting_new_constituent_liquidity")
        >= REVIEWS_TO_RESTORE_LAF
    )
    out = df.with_columns(
        pl.when(qualifies_for_laf & ~restored)
        .then(pl.lit(LAF_VALUE))
        .when(restored)
        .then(pl.lit(1.0))
        .otherwise(pl.col("liquidity_adjustment_factor"))
        .alias("liquidity_adjustment_factor")
    )
    assert float(out["liquidity_adjustment_factor"].min()) >= 0.0, (
        f"negative liquidity_adjustment_factor: min={out['liquidity_adjustment_factor'].min()}"
    )
    assert float(out["liquidity_adjustment_factor"].max()) <= 1.0, (
        f"liquidity_adjustment_factor > 1.0: max={out['liquidity_adjustment_factor'].max()}"
    )
    return out
