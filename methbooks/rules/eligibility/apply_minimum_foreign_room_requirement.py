"""
Purpose: Exclude securities subject to a Foreign Ownership Limit (FOL) whose
foreign room is below 15%.
Datapoints: foreign_room_pct, has_fol_flag.
Thresholds: MIN_FOREIGN_ROOM_PCT=15.
Source: meth-pipeline/MSCI_Global_Investable_Market_Indexes_Methodology_20260331/2026-04-24T07-46-35Z/input/markdown.md section "2.2.8 Minimum Foreign Room Requirement" near line 1237.
See also: methbooks/rules/weighting/apply_initial_foreign_room_adjustment_factor.py (FIF adjustment for foreign room in [15,25)).
"""
from __future__ import annotations

import polars as pl

MIN_FOREIGN_ROOM_PCT = 15.0


def apply_minimum_foreign_room_requirement(df: pl.DataFrame) -> pl.DataFrame:
    assert "foreign_room_pct" in df.columns, (
        f"foreign_room_pct column missing: {df.columns}"
    )
    out = df.filter(
        (~pl.col("has_fol_flag"))
        | (pl.col("foreign_room_pct") >= MIN_FOREIGN_ROOM_PCT)
    )
    bad = out.filter(
        pl.col("has_fol_flag") & (pl.col("foreign_room_pct") < MIN_FOREIGN_ROOM_PCT)
    )
    assert bad.height == 0, (
        f"security with has_fol_flag=True and foreign_room_pct < {MIN_FOREIGN_ROOM_PCT}"
        f" survived: {bad.height} rows;"
        f" min foreign_room_pct={bad['foreign_room_pct'].min()}"
    )
    return out
