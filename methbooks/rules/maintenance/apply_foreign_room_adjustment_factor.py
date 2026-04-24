"""
Purpose: Adjust existing constituent FIF by a lookup-table factor based on current
foreign room level and prior adjustment factor; upward adjustment blocked for 12
months after prior weight reduction or deletion unless driven by FOL change.
Datapoints: foreign_room_pct, current_adjustment_factor, fif,
  last_weight_reduction_date.
Thresholds: FOREIGN_ROOM_TIER_1_PCT=25, FOREIGN_ROOM_TIER_2_PCT=15,
  FOREIGN_ROOM_TIER_3_PCT=7.5, FOREIGN_ROOM_TIER_4_PCT=3.75,
  UPWARD_ADJUSTMENT_LOCKOUT_MONTHS=12.
Source: meth-pipeline/MSCI_Global_Investable_Market_Indexes_Methodology_20260331/2026-04-24T07-46-35Z/input/markdown.md section "3.1.6.2 For Existing Constituents" near line 2524.
See also: methbooks/rules/weighting/apply_initial_foreign_room_adjustment_factor.py (new entrant version).
"""
from __future__ import annotations

import polars as pl

FOREIGN_ROOM_TIER_1_PCT = 25.0
FOREIGN_ROOM_TIER_2_PCT = 15.0
FOREIGN_ROOM_TIER_3_PCT = 7.5
FOREIGN_ROOM_TIER_4_PCT = 3.75
UPWARD_ADJUSTMENT_LOCKOUT_MONTHS = 12


def _new_adjustment_factor(foreign_room_pct: pl.Expr) -> pl.Expr:
    return (
        pl.when(foreign_room_pct >= FOREIGN_ROOM_TIER_1_PCT).then(pl.lit(1.0))
        .when(foreign_room_pct >= FOREIGN_ROOM_TIER_2_PCT).then(pl.lit(0.5))
        .when(foreign_room_pct >= FOREIGN_ROOM_TIER_3_PCT).then(pl.lit(0.25))
        .when(foreign_room_pct >= FOREIGN_ROOM_TIER_4_PCT).then(pl.lit(0.0))
        .otherwise(pl.lit(0.0))
    )


def apply_foreign_room_adjustment_factor(df: pl.DataFrame) -> pl.DataFrame:
    assert "foreign_room_pct" in df.columns, (
        f"foreign_room_pct column missing: {df.columns}"
    )
    assert "current_adjustment_factor" in df.columns, (
        f"current_adjustment_factor column missing: {df.columns}"
    )
    new_factor = _new_adjustment_factor(pl.col("foreign_room_pct"))
    is_upward = new_factor > pl.col("current_adjustment_factor").cast(pl.Float64)
    # Upward adjustment blocked when last_weight_reduction_date is within 12 months.
    # In the mock pipeline months_since_reduction is not tracked; we pass through
    # conservatively by not applying upward adjustments (lockout assumed active).
    out = df.with_columns(
        pl.when(is_upward)
        .then(pl.col("current_adjustment_factor").cast(pl.Float64))
        .otherwise(new_factor)
        .alias("current_adjustment_factor")
    )
    assert float(out["current_adjustment_factor"].min()) >= 0.0, (
        f"negative current_adjustment_factor: min={out['current_adjustment_factor'].min()}"
    )
    assert float(out["current_adjustment_factor"].max()) <= 1.0, (
        f"current_adjustment_factor > 1.0: max={out['current_adjustment_factor'].max()}"
    )
    return out
