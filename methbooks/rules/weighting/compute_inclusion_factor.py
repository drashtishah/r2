"""
Purpose: Derive per-security inclusion factor as ratio of final index weight to pro-forma
    Parent Index market cap weight.
Datapoints: weight, parent_index_weight.
Thresholds: none.
Source: methbooks/data/markdown/msci_quality_indexes_methodology_20250520.md section
    "2.4 Weighting Scheme" near line 240.
"""
from __future__ import annotations

import polars as pl


def compute_inclusion_factor(df: pl.DataFrame) -> pl.DataFrame:
    assert "weight" in df.columns, f"weight missing: {df.columns}"
    assert "parent_index_weight" in df.columns, f"parent_index_weight missing: {df.columns}"

    out = df.with_columns(
        pl.when(pl.col("parent_index_weight") > 0)
        .then(pl.col("weight") / pl.col("parent_index_weight"))
        .otherwise(pl.lit(None).cast(pl.Float64))
        .alias("inclusion_factor")
    )

    assert "inclusion_factor" in out.columns, f"inclusion_factor missing: {out.columns}"

    bad_null = out.filter(
        (pl.col("parent_index_weight") > 0) & pl.col("inclusion_factor").is_null()
    )
    assert bad_null.height == 0, (
        f"null inclusion_factor where parent_index_weight > 0: {bad_null.height} rows"
    )

    bad_val = out.filter(
        (pl.col("parent_index_weight") > 0)
        & ((pl.col("inclusion_factor") - pl.col("weight") / pl.col("parent_index_weight")).abs() > 1e-9)
    )
    assert bad_val.height == 0, (
        f"inclusion_factor != weight / parent_index_weight for {bad_val.height} rows"
    )

    return out
