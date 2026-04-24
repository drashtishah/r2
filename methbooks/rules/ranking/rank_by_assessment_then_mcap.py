"""
Purpose: Rank securities within each GICS sector by security_level_assessment ascending,
    then ff_mcap descending; ties in assessment broken by larger market cap first.
Datapoints: security_level_assessment, ff_mcap, gics_sector.
Thresholds: none.
Source: methbooks/data/markdown/MSCI_Climate_Action_Indexes_Methodology_20251211.md section "Ranking" near line 1.
See also: methbooks/rules/selection/select_top_50pct_with_buffer.py (consumes sector_rank).
"""
from __future__ import annotations

import polars as pl


def rank_by_assessment_then_mcap(df: pl.DataFrame) -> pl.DataFrame:
    # Lower assessment = better rank (rank 1 = assessment 1).
    # For ties in assessment, higher ff_mcap gets lower rank number.
    # struct rank with ascending assessment (descending=False) and descending ff_mcap:
    # polars struct rank uses all fields descending when descending=True.
    # Workaround: negate ff_mcap so that descending sort on struct gives desired order.
    out = df.with_columns(
        (-pl.col("ff_mcap")).alias("_neg_mcap")
    ).with_columns(
        pl.struct(["security_level_assessment", "_neg_mcap"])
        .rank(method="ordinal", descending=False)
        .over("gics_sector")
        .alias("sector_rank")
    ).drop("_neg_mcap")
    assert "sector_rank" in out.columns, f"sector_rank column missing: {out.columns}"
    assert out["sector_rank"].min() >= 1, f"sector_rank below 1: {out['sector_rank'].min()}"
    return out
