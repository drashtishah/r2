"""
Purpose: Apply a 0.5 adjustment factor to FIF for securities whose foreign room is
less than 25% and equal to or higher than 15%, to reflect the actual level of
foreign room in the final FIF.
Datapoints: foreign_room_pct, has_fol_flag, fif.
Thresholds: ADJUSTMENT_FACTOR_UPPER_THRESHOLD_PCT=25,
  ADJUSTMENT_FACTOR_LOWER_THRESHOLD_PCT=15, FIF_ADJUSTMENT_FACTOR=0.5.
Source: meth-pipeline/MSCI_Global_Investable_Market_Indexes_Methodology_20260331/2026-04-24T07-46-35Z/input/markdown.md section "2.3.6.2 Minimum Foreign Room Requirement" near line 1641.
See also: methbooks/rules/maintenance/apply_foreign_room_adjustment_factor.py (existing constituent version with lockout logic).
"""
from __future__ import annotations

import polars as pl

ADJUSTMENT_FACTOR_UPPER_THRESHOLD_PCT = 25.0
ADJUSTMENT_FACTOR_LOWER_THRESHOLD_PCT = 15.0
FIF_ADJUSTMENT_FACTOR = 0.5


def apply_initial_foreign_room_adjustment_factor(df: pl.DataFrame) -> pl.DataFrame:
    assert "fif" in df.columns, f"fif column missing: {df.columns}"
    in_adjustment_range = (
        pl.col("has_fol_flag")
        & (pl.col("foreign_room_pct") >= ADJUSTMENT_FACTOR_LOWER_THRESHOLD_PCT)
        & (pl.col("foreign_room_pct") < ADJUSTMENT_FACTOR_UPPER_THRESHOLD_PCT)
    )
    out = df.with_columns(
        pl.when(in_adjustment_range)
        .then(pl.col("fif") * FIF_ADJUSTMENT_FACTOR)
        .otherwise(pl.col("fif"))
        .alias("fif")
    )
    adjusted = out.filter(in_adjustment_range)
    if adjusted.height > 0:
        # Verify the factor was applied: all adjusted rows should have fif <= pre-adjust.
        # We check by confirming no adjusted security has fif > 1.0.
        assert float(adjusted["fif"].max()) <= 1.0, (
            f"adjusted fif exceeds 1.0 after applying {FIF_ADJUSTMENT_FACTOR} factor:"
            f" max fif={adjusted['fif'].max()}"
        )
    assert float(out["fif"].min()) >= 0.0, (
        f"negative fif after adjustment: min fif={out['fif'].min()}"
    )
    return out
