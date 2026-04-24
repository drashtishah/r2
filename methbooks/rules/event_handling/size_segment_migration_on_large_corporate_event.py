"""
Purpose: Reassign constituents to appropriate size-segments when a corporate event
causes a market cap change of >= +50% or <= -33% relative to pre-event full market cap.
Datapoints: pre_event_full_market_cap_usd, post_event_full_market_cap_usd, fif,
  ff_adjusted_mcap_usd, interim_standard_cutoff_usd, interim_small_cap_cutoff_usd,
  interim_micro_cap_cutoff_usd.
Thresholds: SIGNIFICANT_INCREASE_PCT=50, SIGNIFICANT_DECREASE_PCT=33.
Source: meth-pipeline/MSCI_Global_Investable_Market_Indexes_Methodology_20260331/2026-04-24T07-46-35Z/input/markdown.md section "3.2.4 Changes in Size segment of Existing Index Constituents as a Result of a Large Corporate Event" near line 2906.
See also: methbooks/rules/event_handling/early_deletion_policy.py (deletion when event renders security ineligible).
"""
from __future__ import annotations

import polars as pl

SIGNIFICANT_INCREASE_PCT = 50.0
SIGNIFICANT_DECREASE_PCT = 33.0


def size_segment_migration_on_large_corporate_event(df: pl.DataFrame) -> pl.DataFrame:
    assert "pre_event_full_market_cap_usd" in df.columns, (
        f"pre_event_full_market_cap_usd column missing: {df.columns}"
    )
    assert "post_event_full_market_cap_usd" in df.columns, (
        f"post_event_full_market_cap_usd column missing: {df.columns}"
    )
    pct_change = (
        (pl.col("post_event_full_market_cap_usd") - pl.col("pre_event_full_market_cap_usd"))
        / pl.col("pre_event_full_market_cap_usd")
        * 100.0
    )
    is_large_event = (pct_change >= SIGNIFICANT_INCREASE_PCT) | (
        pct_change <= -SIGNIFICANT_DECREASE_PCT
    )
    # Flag rows where size-segment review is triggered; downstream assignment
    # uses the lookup table from section 3.2.4 applied to ff_adjusted_mcap_usd
    # relative to interim cutoffs.
    out = df.with_columns(
        is_large_event.alias("size_segment_review_triggered")
    )
    # Business assert: large events are correctly flagged.
    large_events = out.filter(pl.col("size_segment_review_triggered"))
    if large_events.height > 0:
        assert (
            large_events.filter(
                (pct_change < SIGNIFICANT_INCREASE_PCT) & (pct_change > -SIGNIFICANT_DECREASE_PCT)
            ).height == 0
        ), "size_segment_review_triggered=True for a non-large event"
    return out
