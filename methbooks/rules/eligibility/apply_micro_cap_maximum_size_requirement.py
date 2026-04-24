"""
Purpose: Exclude from Micro Cap any company with full market cap exceeding the
Small Cap Entry Buffer (1.5x IMI Market Size-Segment Cutoff) to maintain clean
boundary between IMI and Micro Cap.
Datapoints: full_market_cap_usd, imi_market_size_segment_cutoff_usd.
Thresholds: SMALL_CAP_ENTRY_BUFFER_MULTIPLIER=1.5.
Source: meth-pipeline/MSCI_Global_Investable_Market_Indexes_Methodology_20260331/2026-04-24T07-46-35Z/input/markdown.md section "4.1.1 Micro Cap Maximum Size Requirement" near line 3328.
See also: methbooks/rules/eligibility/apply_micro_cap_minimum_size_requirement.py (Micro Cap lower bound).
"""
from __future__ import annotations

import polars as pl

SMALL_CAP_ENTRY_BUFFER_MULTIPLIER = 1.5


def apply_micro_cap_maximum_size_requirement(df: pl.DataFrame) -> pl.DataFrame:
    assert "full_market_cap_usd" in df.columns, (
        f"full_market_cap_usd column missing: {df.columns}"
    )
    assert "imi_market_size_segment_cutoff_usd" in df.columns, (
        f"imi_market_size_segment_cutoff_usd column missing: {df.columns}"
    )
    ceiling = SMALL_CAP_ENTRY_BUFFER_MULTIPLIER * pl.col("imi_market_size_segment_cutoff_usd")
    out = df.filter(pl.col("full_market_cap_usd") <= ceiling)
    bad = out.filter(pl.col("full_market_cap_usd") > ceiling)
    assert bad.height == 0, (
        f"Micro Cap company with full_market_cap_usd > "
        f"{SMALL_CAP_ENTRY_BUFFER_MULTIPLIER}x imi_market_size_segment_cutoff_usd"
        f" survived: {bad.height} rows;"
        f" max full_market_cap_usd={bad['full_market_cap_usd'].max()}"
    )
    return out
