"""
Purpose: Exclude MSCI China Equity Universe securities that are suspended on the
Price Cutoff Date or have been suspended for 50 or more consecutive business days
in the past 12 months.
Datapoints: market_type, china_suspension_flag,
  china_suspension_consecutive_business_days.
Thresholds: CHINA_SUSPENSION_CONSECUTIVE_DAYS_MAX=50.
Source: meth-pipeline/MSCI_Global_Investable_Market_Indexes_Methodology_20260331/2026-04-24T07-46-35Z/input/markdown.md section "2.2.5 DM and EM Minimum Liquidity Requirement" near line 1078.
See also: methbooks/rules/event_handling/early_deletion_policy.py (prolonged suspension deletion).
"""
from __future__ import annotations

import polars as pl

CHINA_SUSPENSION_CONSECUTIVE_DAYS_MAX = 50
_CHINA_MARKET = "China"


def apply_china_suspension_screen(df: pl.DataFrame) -> pl.DataFrame:
    assert "china_suspension_flag" in df.columns, (
        f"china_suspension_flag column missing: {df.columns}"
    )
    assert "china_suspension_consecutive_business_days" in df.columns, (
        f"china_suspension_consecutive_business_days column missing: {df.columns}"
    )
    is_china = pl.col("market_type") == _CHINA_MARKET
    fails = (
        pl.col("china_suspension_flag")
        | (
            pl.col("china_suspension_consecutive_business_days")
            >= CHINA_SUSPENSION_CONSECUTIVE_DAYS_MAX
        )
    )
    out = df.filter(~(is_china & fails))
    bad = out.filter(is_china & fails)
    assert bad.height == 0, (
        f"China security with suspension flag or >= {CHINA_SUSPENSION_CONSECUTIVE_DAYS_MAX}"
        f" consecutive suspended days survived: {bad.height} rows"
    )
    return out
