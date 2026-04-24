"""
Purpose: Exclude from Micro Cap securities that fail the Micro Cap liquidity screen:
12-month ATVR >= 5% and 12-month frequency of trading >= 50%.
Datapoints: atvr_12m_pct, freq_of_trading_12m_pct, atvr_3m_available_flag.
Thresholds: MIN_ATVR_12M_PCT=5, MIN_FREQ_12M_PCT=50.
Source: meth-pipeline/MSCI_Global_Investable_Market_Indexes_Methodology_20260331/2026-04-24T07-46-35Z/input/markdown.md section "4.1.3 Micro Cap Minimum Liquidity Requirement" near line 3355.
See also: methbooks/rules/eligibility/apply_dm_em_minimum_liquidity_requirement.py (DM/EM liquidity screen with higher thresholds).
"""
from __future__ import annotations

import polars as pl

MIN_ATVR_12M_PCT = 5.0
MIN_FREQ_12M_PCT = 50.0


def apply_micro_cap_minimum_liquidity_requirement(df: pl.DataFrame) -> pl.DataFrame:
    assert "atvr_12m_pct" in df.columns, f"atvr_12m_pct column missing: {df.columns}"
    assert "freq_of_trading_12m_pct" in df.columns, (
        f"freq_of_trading_12m_pct column missing: {df.columns}"
    )
    out = df.filter(
        (pl.col("atvr_12m_pct") >= MIN_ATVR_12M_PCT)
        & (pl.col("freq_of_trading_12m_pct") >= MIN_FREQ_12M_PCT)
    )
    bad_atvr = out.filter(pl.col("atvr_12m_pct") < MIN_ATVR_12M_PCT)
    assert bad_atvr.height == 0, (
        f"Micro Cap security with atvr_12m_pct < {MIN_ATVR_12M_PCT} survived:"
        f" {bad_atvr.height} rows; min atvr_12m_pct={bad_atvr['atvr_12m_pct'].min()}"
    )
    bad_freq = out.filter(pl.col("freq_of_trading_12m_pct") < MIN_FREQ_12M_PCT)
    assert bad_freq.height == 0, (
        f"Micro Cap security with freq_of_trading_12m_pct < {MIN_FREQ_12M_PCT} survived:"
        f" {bad_freq.height} rows;"
        f" min freq_of_trading_12m_pct={bad_freq['freq_of_trading_12m_pct'].min()}"
    )
    return out
