"""
Purpose: Review and update GICS classification when a non-index constituent or
Micro Cap constituent is added to GIMI, or a Small Cap constituent migrates up
to Standard; also reviews parent classification at spin-off.
Datapoints: gics_sub_industry, size_segment, event_type.
Thresholds: none.
Source: meth-pipeline/MSCI_Global_Investable_Market_Indexes_Methodology_20260331/2026-04-24T07-46-35Z/input/markdown.md section "3.2.5 Changes in Style Segment or Industry Classification due to the Corporate Event" near line 3090.
See also: methbooks/rules/maintenance/gics_non_event_change_announcement_schedule.py (non-event GICS changes).
"""
from __future__ import annotations

import polars as pl

_REVIEW_TRIGGERING_EVENTS = {"new_addition", "micro_cap_upgrade", "small_cap_to_standard", "spinoff"}


def gics_classification_review_at_event(df: pl.DataFrame) -> pl.DataFrame:
    assert "gics_sub_industry" in df.columns, (
        f"gics_sub_industry column missing: {df.columns}"
    )
    # Flag rows where GICS should be reviewed (used by downstream GICS assignment).
    out = df.with_columns(
        pl.col("event_type").is_in(list(_REVIEW_TRIGGERING_EVENTS)).alias(
            "gics_review_required"
        )
    )
    # Business assert: GICS review not triggered for securities staying in same segment.
    same_segment_no_review = out.filter(
        (pl.col("event_type") == "same_segment_migration")
        & pl.col("gics_review_required")
    )
    assert same_segment_no_review.height == 0, (
        f"gics_review_required=True for same_segment_migration event:"
        f" {same_segment_no_review.height} rows"
    )
    return out
