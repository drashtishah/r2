"""
Purpose: Assign sector-relative quartile intensity score (1-4); score 4 = highest emission intensity.
Datapoints: emission_intensity, ff_mcap, gics_sector.
Thresholds: none (quartile boundaries computed per sector).
Source: methbooks/data/markdown/MSCI_Climate_Action_Indexes_Methodology_20251211.md section "2.3.1 Security-Level Assessment" near line 206.
See also: methbooks/rules/scoring/score_climate_risk_management.py (companion scoring rule).
"""
from __future__ import annotations

import polars as pl


def score_intensity(df: pl.DataFrame) -> pl.DataFrame:
    # Higher emission_intensity -> score 4. Sort descending by emission_intensity,
    # then descending by ff_mcap for tie-breaking. rank=1 -> score=4.
    out = (
        df.with_columns(
            pl.struct(["emission_intensity", "ff_mcap"])
            .rank(method="ordinal", descending=True)
            .over("gics_sector")
            .alias("_rank")
        )
        .with_columns(
            pl.col("gics_sector").count().over("gics_sector").alias("_n")
        )
        .with_columns(
            (4 - (((pl.col("_rank") - 1) * 4) // pl.col("_n"))).cast(pl.Int32).alias("intensity_score")
        )
        .drop(["_rank", "_n"])
    )
    assert "intensity_score" in out.columns, f"intensity_score column missing: {out.columns}"
    assert out["intensity_score"].min() >= 1, f"intensity_score below 1: {out['intensity_score'].min()}"
    assert out["intensity_score"].max() <= 4, f"intensity_score above 4: {out['intensity_score'].max()}"
    return out
