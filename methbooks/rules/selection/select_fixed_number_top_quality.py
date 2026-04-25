"""
Purpose: Retain only the top-N securities by quality rank at initial construction.
Datapoints: quality_rank, quality_score, fixed_number_securities.
Thresholds: none.
Source: methbooks/data/markdown/msci_quality_indexes_methodology_20250520.md section
    "2.3 Security Selection" near line 216.
"""
from __future__ import annotations

import polars as pl


def select_fixed_number_top_quality(df: pl.DataFrame) -> pl.DataFrame:
    assert "quality_rank" in df.columns, f"quality_rank missing: {df.columns}"
    assert "quality_score" in df.columns, f"quality_score missing: {df.columns}"
    assert "fixed_number_securities" in df.columns, (
        f"fixed_number_securities missing: {df.columns}"
    )

    fixed_n = int(df["fixed_number_securities"][0])

    eligible = df.filter(
        pl.col("quality_score").is_not_null() & (pl.col("quality_score") > 0)
    )

    if eligible.height <= fixed_n:
        out = eligible
    else:
        out = eligible.filter(pl.col("quality_rank") <= fixed_n)

    assert out["quality_score"].null_count() == 0, (
        f"null quality_score in output: {out['quality_score'].null_count()} rows"
    )
    assert out.filter(pl.col("quality_score") <= 0).height == 0, (
        f"non-positive quality_score in output: "
        f"{out.filter(pl.col('quality_score') <= 0).height} rows"
    )
    return out
