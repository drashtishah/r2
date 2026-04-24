"""
Purpose: Flag securities with a credible emission reduction track record (track_record_score == 1).
Datapoints: track_record_score.
Thresholds: track_record_score == 1.
Source: methbooks/data/markdown/MSCI_Climate_Action_Indexes_Methodology_20251211.md section "Scoring" near line 1.
See also: methbooks/rules/scoring/score_emission_track_record.py (source of track_record_score).
"""
from __future__ import annotations

import polars as pl


def assess_credible_track_record(df: pl.DataFrame) -> pl.DataFrame:
    out = df.with_columns(
        pl.when(pl.col("track_record_score").is_null())
        .then(pl.lit(None).cast(pl.Boolean))
        .when(pl.col("track_record_score") == 1)
        .then(pl.lit(True))
        .otherwise(pl.lit(False))
        .alias("credible_track_record")
    )
    assert "credible_track_record" in out.columns, (
        f"credible_track_record column missing: {out.columns}"
    )
    return out
