"""
Purpose: Implement NOS changes at quarterly reviews when absolute NOS change
>= 1,000 shares or relative change >= 0.02%; changes after Price Cutoff Date
deferred to next review.
Datapoints: current_nos, updated_nos.
Thresholds: MIN_ABS_NOS_CHANGE=1000, MIN_REL_NOS_CHANGE_PCT=0.02.
Source: meth-pipeline/MSCI_Global_Investable_Market_Indexes_Methodology_20260331/2026-04-24T07-46-35Z/input/markdown.md section "3.1.8 Index Review of Changes in Number of Shares (NOS)" near line 2654.
See also: methbooks/rules/event_handling/early_deletion_policy.py (intra-review NOS handled via corporate events).
"""
from __future__ import annotations

import polars as pl

MIN_ABS_NOS_CHANGE = 1_000
MIN_REL_NOS_CHANGE_PCT = 0.02


def apply_nos_change_at_review(df: pl.DataFrame) -> pl.DataFrame:
    assert "current_nos" in df.columns, f"current_nos column missing: {df.columns}"
    assert "updated_nos" in df.columns, f"updated_nos column missing: {df.columns}"
    abs_change = (pl.col("updated_nos") - pl.col("current_nos")).abs()
    rel_change_pct = abs_change / pl.col("current_nos") * 100.0
    threshold_met = (abs_change >= MIN_ABS_NOS_CHANGE) | (
        rel_change_pct >= MIN_REL_NOS_CHANGE_PCT
    )
    out = df.with_columns(
        pl.when(threshold_met)
        .then(pl.col("updated_nos"))
        .otherwise(pl.col("current_nos"))
        .alias("current_nos")
    )
    assert float(out["current_nos"].min()) > 0, (
        f"non-positive current_nos after update: min={out['current_nos'].min()}"
    )
    # Business assert: NOS updated when abs change >= 1000 or relative change >= 0.02%.
    updated_rows = out.filter(
        (abs_change >= MIN_ABS_NOS_CHANGE) | (rel_change_pct >= MIN_REL_NOS_CHANGE_PCT)
    )
    unchanged_rows = out.filter(
        (abs_change < MIN_ABS_NOS_CHANGE) & (rel_change_pct < MIN_REL_NOS_CHANGE_PCT)
    )
    if unchanged_rows.height > 0:
        assert (
            (unchanged_rows["current_nos"] == df.filter(
                (abs_change < MIN_ABS_NOS_CHANGE)
                & (rel_change_pct < MIN_REL_NOS_CHANGE_PCT)
            )["current_nos"]).all()
        ), "NOS changed for rows below threshold"
    return out
