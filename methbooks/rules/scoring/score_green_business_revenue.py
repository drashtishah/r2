"""
Purpose: Assign sector-relative quartile green business score (1-4); score 4 = highest green revenue.
Datapoints: green_business_revenue_pct, ff_mcap, gics_sector.
Thresholds: none (quartile boundaries computed per sector).
Source: methbooks/data/markdown/MSCI_Climate_Action_Indexes_Methodology_20251211.md section "Scoring" near line 1.
See also: methbooks/rules/scoring/score_intensity.py (companion scoring rule).
"""
from __future__ import annotations

import polars as pl


def score_green_business_revenue(df: pl.DataFrame) -> pl.DataFrame:
    # Higher green_business_revenue_pct -> score 4. Descending rank; rank=1 -> score=4.
    out = (
        df.with_columns(
            pl.struct(["green_business_revenue_pct", "ff_mcap"])
            .rank(method="ordinal", descending=True)
            .over("gics_sector")
            .alias("_rank")
        )
        .with_columns(
            pl.col("gics_sector").count().over("gics_sector").alias("_n")
        )
        .with_columns(
            (4 - (((pl.col("_rank") - 1) * 4) // pl.col("_n"))).cast(pl.Int32).alias("green_business_score")
        )
        .drop(["_rank", "_n"])
    )
    assert "green_business_score" in out.columns, f"green_business_score column missing: {out.columns}"
    assert out["green_business_score"].min() >= 1, f"green_business_score below 1: {out['green_business_score'].min()}"
    assert out["green_business_score"].max() <= 4, f"green_business_score above 4: {out['green_business_score'].max()}"
    return out
