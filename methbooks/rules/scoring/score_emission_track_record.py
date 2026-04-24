"""
Purpose: Assign sector-relative quartile emission track record score (1-4, nullable);
    score 1 = strongest historical emission reduction; null for securities without a track record.
Datapoints: emission_track_record, ff_mcap, gics_sector.
Thresholds: none (quartile boundaries computed per sector among eligible rows).
Source: methbooks/data/markdown/MSCI_Climate_Action_Indexes_Methodology_20251211.md section "Appendix II: Calculation of Security Level Scores" near line 720.
See also: methbooks/rules/scoring/assess_credible_track_record.py (derived from this score).
"""
from __future__ import annotations

import polars as pl


def score_emission_track_record(df: pl.DataFrame) -> pl.DataFrame:
    # Only assign score to rows with non-null emission_track_record.
    # Higher (less negative) emission_track_record -> score 4 (weakest reduction).
    # Lower (more negative) -> score 1 (strongest reduction).
    # Descending rank by emission_track_record then ff_mcap; rank=1 -> score=4.
    eligible = df.filter(pl.col("emission_track_record").is_not_null())
    ineligible = df.filter(pl.col("emission_track_record").is_null())

    if eligible.height > 0:
        scored = (
            eligible.with_columns(
                pl.struct(["emission_track_record", "ff_mcap"])
                .rank(method="ordinal", descending=True)
                .over("gics_sector")
                .alias("_rank")
            )
            .with_columns(
                pl.col("gics_sector").count().over("gics_sector").alias("_n")
            )
            .with_columns(
                (4 - (((pl.col("_rank") - 1) * 4) // pl.col("_n"))).cast(pl.Int32).alias("track_record_score")
            )
            .drop(["_rank", "_n"])
        )
    else:
        scored = eligible.with_columns(
            pl.lit(None).cast(pl.Int32).alias("track_record_score")
        )

    ineligible = ineligible.with_columns(
        pl.lit(None).cast(pl.Int32).alias("track_record_score")
    )

    out = pl.concat([scored, ineligible])
    assert "track_record_score" in out.columns, f"track_record_score column missing: {out.columns}"
    non_null = out["track_record_score"].drop_nulls()
    if non_null.len() > 0:
        assert non_null.min() >= 1, f"track_record_score below 1: {non_null.min()}"
        assert non_null.max() <= 4, f"track_record_score above 4: {non_null.max()}"
    return out
