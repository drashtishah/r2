"""
Purpose: Implement FIF changes at quarterly reviews only when the absolute change
in free float exceeds thresholds that vary by current free float level, to limit
turnover from small float corrections.
Datapoints: free_float_pct, current_dif_pct, updated_free_float_pct,
  is_addition_flag, fol_change_flag.
Thresholds: HIGH_FLOAT_THRESHOLD_PCT=25, MID_FLOAT_LOWER_PCT=5,
  HIGH_FLOAT_MIN_ABS_CHANGE_PCT=2.5, MID_FLOAT_MIN_ABS_CHANGE_PCT=0.5,
  LOW_FLOAT_MIN_ABS_CHANGE_PCT=0.1.
Source: meth-pipeline/MSCI_Global_Investable_Market_Indexes_Methodology_20260331/2026-04-24T07-46-35Z/input/markdown.md section "3.1.7 Index Review of Changes in Foreign Inclusion Factors (FIFs)" near line 2594.
See also: methbooks/rules/maintenance/apply_foreign_room_adjustment_factor.py (companion FIF adjustment for foreign room).
"""
from __future__ import annotations

import polars as pl

HIGH_FLOAT_THRESHOLD_PCT = 25.0
MID_FLOAT_LOWER_PCT = 5.0
HIGH_FLOAT_MIN_ABS_CHANGE_PCT = 2.5
MID_FLOAT_MIN_ABS_CHANGE_PCT = 0.5
LOW_FLOAT_MIN_ABS_CHANGE_PCT = 0.1


def apply_fif_change_at_review(df: pl.DataFrame) -> pl.DataFrame:
    assert "free_float_pct" in df.columns, (
        f"free_float_pct column missing: {df.columns}"
    )
    assert "current_dif_pct" in df.columns, (
        f"current_dif_pct column missing: {df.columns}"
    )
    abs_change = (pl.col("updated_free_float_pct") - pl.col("free_float_pct")).abs()
    is_high_float = pl.col("free_float_pct") >= HIGH_FLOAT_THRESHOLD_PCT
    is_mid_float = (pl.col("free_float_pct") >= MID_FLOAT_LOWER_PCT) & (
        pl.col("free_float_pct") < HIGH_FLOAT_THRESHOLD_PCT
    )
    is_low_float = pl.col("free_float_pct") < MID_FLOAT_LOWER_PCT
    threshold_met = (
        (is_high_float & (abs_change >= HIGH_FLOAT_MIN_ABS_CHANGE_PCT))
        | (is_mid_float & (abs_change >= MID_FLOAT_MIN_ABS_CHANGE_PCT))
        | (is_low_float & (abs_change >= LOW_FLOAT_MIN_ABS_CHANGE_PCT))
        | pl.col("is_addition_flag")
        | pl.col("fol_change_flag")
    )
    # Apply updated free float only when threshold is met; otherwise keep current.
    out = df.with_columns(
        pl.when(threshold_met)
        .then(pl.col("updated_free_float_pct"))
        .otherwise(pl.col("free_float_pct"))
        .alias("free_float_pct")
    )
    assert float(out["free_float_pct"].min()) >= 0.0, (
        f"negative free_float_pct after update: min={out['free_float_pct'].min()}"
    )
    assert float(out["free_float_pct"].max()) <= 100.0, (
        f"free_float_pct exceeds 100 after update: max={out['free_float_pct'].max()}"
    )
    return out
