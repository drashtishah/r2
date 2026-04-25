"""
Purpose: Transform composite Quality Z-Score into Quality Score using asymmetric formula
    that amplifies high-quality and compresses low-quality securities.
Datapoints: composite_quality_z_score.
Thresholds: none.
Source: methbooks/data/markdown/msci_quality_indexes_methodology_20250520.md section
    "2.2.3 Calculating the Quality Score" near line 199.
"""
from __future__ import annotations

import polars as pl


def compute_quality_score_from_z(df: pl.DataFrame) -> pl.DataFrame:
    assert "composite_quality_z_score" in df.columns, (
        f"composite_quality_z_score missing: {df.columns}"
    )

    out = df.with_columns(
        pl.when(pl.col("composite_quality_z_score").is_null())
        .then(pl.lit(None).cast(pl.Float64))
        .when(pl.col("composite_quality_z_score") > 0)
        .then(1.0 + pl.col("composite_quality_z_score"))
        .when(pl.col("composite_quality_z_score") < 0)
        .then(1.0 / (1.0 - pl.col("composite_quality_z_score")))
        .otherwise(pl.lit(1.0))
        .alias("quality_score")
    )

    assert "quality_score" in out.columns, f"quality_score missing: {out.columns}"

    non_null_composite = out.filter(pl.col("composite_quality_z_score").is_not_null())
    assert non_null_composite.filter(pl.col("quality_score").is_null()).height == 0, (
        f"null quality_score where composite is non-null: "
        f"{non_null_composite.filter(pl.col('quality_score').is_null()).height} rows"
    )

    min_qs = float(out["quality_score"].drop_nulls().min() or 0.0)
    assert min_qs > 0, f"non-positive quality_score: {min_qs}"

    bad_pos = out.filter(
        pl.col("composite_quality_z_score").is_not_null()
        & (pl.col("composite_quality_z_score") > 0)
        & (pl.col("quality_score") <= 1.0)
    )
    assert bad_pos.height == 0, (
        f"quality_score not > 1 when Z > 0: {bad_pos.height} rows"
    )

    bad_neg = out.filter(
        pl.col("composite_quality_z_score").is_not_null()
        & (pl.col("composite_quality_z_score") < 0)
        & (pl.col("quality_score") >= 1.0)
    )
    assert bad_neg.height == 0, (
        f"quality_score not < 1 when Z < 0: {bad_neg.height} rows"
    )

    return out
