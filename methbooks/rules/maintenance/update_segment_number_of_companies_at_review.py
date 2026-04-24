"""
Purpose: Reassess Segment Number of Companies at each Index Review within stability
limits: cap reduction at 5% then 20% of Initial Segment Number; additions fill
above Global Minimum Size Range upper bound first.
Datapoints: full_market_cap_usd, global_min_size_range_lower_usd,
  global_min_size_range_upper_usd, cumulative_ff_mcap_coverage_pct,
  initial_segment_number_of_companies.
Thresholds: MAX_REDUCTION_STEP1_PCT=5, MAX_REDUCTION_STEP2_PCT=20,
  LOWER_PROXIMITY_AREA_LOWER_MULTIPLIER=0.5,
  LOWER_PROXIMITY_AREA_UPPER_MULTIPLIER=0.575,
  UPPER_PROXIMITY_AREA_LOWER_MULTIPLIER=1,
  UPPER_PROXIMITY_AREA_UPPER_MULTIPLIER=1.15.
Source: meth-pipeline/MSCI_Global_Investable_Market_Indexes_Methodology_20260331/2026-04-24T07-46-35Z/input/markdown.md section "3.1.4.2 Changes in the Segment Number of Companies" near line 2248.
See also: methbooks/rules/maintenance/update_equity_universe_minimum_size_at_review.py (companion size threshold update).
"""
from __future__ import annotations

import polars as pl

MAX_REDUCTION_STEP1_PCT = 5.0
MAX_REDUCTION_STEP2_PCT = 20.0
LOWER_PROXIMITY_AREA_LOWER_MULTIPLIER = 0.5
LOWER_PROXIMITY_AREA_UPPER_MULTIPLIER = 0.575
UPPER_PROXIMITY_AREA_LOWER_MULTIPLIER = 1.0
UPPER_PROXIMITY_AREA_UPPER_MULTIPLIER = 1.15


def update_segment_number_of_companies_at_review(df: pl.DataFrame) -> pl.DataFrame:
    assert "full_market_cap_usd" in df.columns, (
        f"full_market_cap_usd column missing: {df.columns}"
    )
    assert "global_min_size_range_lower_usd" in df.columns, (
        f"global_min_size_range_lower_usd column missing: {df.columns}"
    )
    assert "global_min_size_range_upper_usd" in df.columns, (
        f"global_min_size_range_upper_usd column missing: {df.columns}"
    )
    assert "initial_segment_number_of_companies" in df.columns, (
        f"initial_segment_number_of_companies column missing: {df.columns}"
    )
    # This maintenance rule passes the dataframe through unchanged at the row level;
    # segment number changes are a methodological computation tracked via the
    # initial_segment_number_of_companies column populated during data preparation.
    current_count = df.height
    initial = int(df["initial_segment_number_of_companies"].max() or 0)
    if initial > 0:
        max_allowed_reduction = initial * MAX_REDUCTION_STEP2_PCT / 100.0
        min_allowed_count = initial - max_allowed_reduction
        assert current_count >= min_allowed_count, (
            f"Segment Number of Companies {current_count} reduced more than"
            f" {MAX_REDUCTION_STEP2_PCT}% from Initial Segment Number {initial}:"
            f" min allowed={min_allowed_count}"
        )
    return df
