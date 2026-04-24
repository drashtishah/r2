"""
Purpose: Exclude securities that fail the DM or EM Minimum Liquidity Requirement
measured by 12-month ATVR, 3-month ATVR, and 3-month Frequency of Trading over
the last 4 consecutive quarters.
Datapoints: atvr_12m_pct, atvr_3m_pct, freq_of_trading_3m_pct, market_type,
  is_existing_imi_constituent.
Thresholds: DM_ATVR_12M_MIN_PCT=20, DM_ATVR_3M_MIN_PCT=20,
  DM_FREQ_3M_MIN_PCT=90, EM_ATVR_12M_MIN_PCT=15, EM_ATVR_3M_MIN_PCT=15,
  EM_FREQ_3M_MIN_PCT=80, EXISTING_CONSTITUENT_DM_ATVR_12M_FLOOR_PCT=13.3,
  EXISTING_CONSTITUENT_EM_ATVR_12M_FLOOR_PCT=10,
  EXISTING_CONSTITUENT_ATVR_3M_MIN_PCT=5,
  EXISTING_CONSTITUENT_DM_FREQ_3M_MIN_PCT=80,
  EXISTING_CONSTITUENT_EM_FREQ_3M_MIN_PCT=70.
Source: meth-pipeline/MSCI_Global_Investable_Market_Indexes_Methodology_20260331/2026-04-24T07-46-35Z/input/markdown.md section "2.2.5 DM and EM Minimum Liquidity Requirement" near line 1039.
See also: methbooks/rules/eligibility/apply_fm_minimum_liquidity_requirement.py (FM-specific tiered liquidity screen).
"""
from __future__ import annotations

import polars as pl

DM_ATVR_12M_MIN_PCT = 20.0
DM_ATVR_3M_MIN_PCT = 20.0
DM_FREQ_3M_MIN_PCT = 90.0
EM_ATVR_12M_MIN_PCT = 15.0
EM_ATVR_3M_MIN_PCT = 15.0
EM_FREQ_3M_MIN_PCT = 80.0
EXISTING_CONSTITUENT_DM_ATVR_12M_FLOOR_PCT = 13.3
EXISTING_CONSTITUENT_EM_ATVR_12M_FLOOR_PCT = 10.0
EXISTING_CONSTITUENT_ATVR_3M_MIN_PCT = 5.0
EXISTING_CONSTITUENT_DM_FREQ_3M_MIN_PCT = 80.0
EXISTING_CONSTITUENT_EM_FREQ_3M_MIN_PCT = 70.0


def apply_dm_em_minimum_liquidity_requirement(df: pl.DataFrame) -> pl.DataFrame:
    for col in ("atvr_12m_pct", "atvr_3m_pct", "freq_of_trading_3m_pct"):
        assert col in df.columns, f"{col} column missing: {df.columns}"

    is_dm = pl.col("market_type") == "DM"
    is_em = pl.col("market_type") == "EM"
    is_new = ~pl.col("is_existing_imi_constituent")
    is_existing = pl.col("is_existing_imi_constituent")

    # Non-DM/EM rows pass through untouched.
    passes_dm_new = is_dm & is_new & (
        (pl.col("atvr_12m_pct") >= DM_ATVR_12M_MIN_PCT)
        & (pl.col("atvr_3m_pct") >= DM_ATVR_3M_MIN_PCT)
        & (pl.col("freq_of_trading_3m_pct") >= DM_FREQ_3M_MIN_PCT)
    )
    passes_em_new = is_em & is_new & (
        (pl.col("atvr_12m_pct") >= EM_ATVR_12M_MIN_PCT)
        & (pl.col("atvr_3m_pct") >= EM_ATVR_3M_MIN_PCT)
        & (pl.col("freq_of_trading_3m_pct") >= EM_FREQ_3M_MIN_PCT)
    )
    passes_dm_existing = is_dm & is_existing & (
        (pl.col("atvr_12m_pct") >= EXISTING_CONSTITUENT_DM_ATVR_12M_FLOOR_PCT)
        & (pl.col("atvr_3m_pct") >= EXISTING_CONSTITUENT_ATVR_3M_MIN_PCT)
        & (pl.col("freq_of_trading_3m_pct") >= EXISTING_CONSTITUENT_DM_FREQ_3M_MIN_PCT)
    )
    passes_em_existing = is_em & is_existing & (
        (pl.col("atvr_12m_pct") >= EXISTING_CONSTITUENT_EM_ATVR_12M_FLOOR_PCT)
        & (pl.col("atvr_3m_pct") >= EXISTING_CONSTITUENT_ATVR_3M_MIN_PCT)
        & (pl.col("freq_of_trading_3m_pct") >= EXISTING_CONSTITUENT_EM_FREQ_3M_MIN_PCT)
    )
    not_dm_or_em = ~(is_dm | is_em)

    out = df.filter(
        passes_dm_new
        | passes_em_new
        | passes_dm_existing
        | passes_em_existing
        | not_dm_or_em
    )

    # Business assert: no non-constituent DM fails new-entrant thresholds.
    bad_dm_new = out.filter(
        is_dm & is_new & (
            (pl.col("atvr_12m_pct") < DM_ATVR_12M_MIN_PCT)
            | (pl.col("atvr_3m_pct") < DM_ATVR_3M_MIN_PCT)
            | (pl.col("freq_of_trading_3m_pct") < DM_FREQ_3M_MIN_PCT)
        )
    )
    assert bad_dm_new.height == 0, (
        f"non-constituent DM security failing liquidity survived: {bad_dm_new.height} rows"
    )
    bad_em_new = out.filter(
        is_em & is_new & (
            (pl.col("atvr_12m_pct") < EM_ATVR_12M_MIN_PCT)
            | (pl.col("atvr_3m_pct") < EM_ATVR_3M_MIN_PCT)
            | (pl.col("freq_of_trading_3m_pct") < EM_FREQ_3M_MIN_PCT)
        )
    )
    assert bad_em_new.height == 0, (
        f"non-constituent EM security failing liquidity survived: {bad_em_new.height} rows"
    )
    return out
