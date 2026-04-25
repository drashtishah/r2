"""
Purpose: Average the three variable Z-scores into a composite; use two-variable fallback
    when earnings_variability_z_score is null.
Datapoints: roe_z_score, debt_to_equity_z_score, earnings_variability_z_score.
Thresholds: none.
Source: methbooks/data/markdown/msci_quality_indexes_methodology_20250520.md section
    "2.2.3 Calculating the Quality Score" near line 193.
"""
from __future__ import annotations

import polars as pl


def compute_composite_quality_z_score(df: pl.DataFrame) -> pl.DataFrame:
    assert "roe_z_score" in df.columns, f"roe_z_score missing: {df.columns}"
    assert "debt_to_equity_z_score" in df.columns, f"debt_to_equity_z_score missing: {df.columns}"
    assert "earnings_variability_z_score" in df.columns, (
        f"earnings_variability_z_score missing: {df.columns}"
    )

    out = df.with_columns(
        pl.when(
            pl.col("roe_z_score").is_null() | pl.col("debt_to_equity_z_score").is_null()
        )
        .then(pl.lit(None).cast(pl.Float64))
        .when(pl.col("earnings_variability_z_score").is_not_null())
        .then(
            (pl.col("roe_z_score") + pl.col("debt_to_equity_z_score") + pl.col("earnings_variability_z_score")) / 3.0
        )
        .otherwise(
            (pl.col("roe_z_score") + pl.col("debt_to_equity_z_score")) / 2.0
        )
        .alias("composite_quality_z_score")
    )

    assert "composite_quality_z_score" in out.columns, (
        f"composite_quality_z_score missing: {out.columns}"
    )

    # Rows with null roe_z or de_z must have null composite.
    null_mandatory = out.filter(
        pl.col("roe_z_score").is_null() | pl.col("debt_to_equity_z_score").is_null()
    )
    bad_nonnull = null_mandatory.filter(pl.col("composite_quality_z_score").is_not_null())
    assert bad_nonnull.height == 0, (
        f"composite not null when mandatory z-score is null: {bad_nonnull.height} rows"
    )

    # Rows with non-null ev_z use three-variable average.
    three_var = out.filter(
        pl.col("roe_z_score").is_not_null()
        & pl.col("debt_to_equity_z_score").is_not_null()
        & pl.col("earnings_variability_z_score").is_not_null()
    )
    bad_three = three_var.filter(
        (
            (pl.col("composite_quality_z_score")
             - (pl.col("roe_z_score") + pl.col("debt_to_equity_z_score") + pl.col("earnings_variability_z_score")) / 3.0
            ).abs() > 1e-9
        )
    )
    assert bad_three.height == 0, (
        f"three-variable composite incorrect for {bad_three.height} rows"
    )

    return out
