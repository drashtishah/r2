"""
Purpose: Compute short-term forward EPS growth rate as (EPS12F - EPS12B) / |EPS12B|, measuring expected 12-month earnings growth.
Datapoints: eps12f, eps12b.
Thresholds: none.
Source: methbooks/data/markdown/MSCI_Fundamental_Data_Methodology_20240625.md section "2.2.5 Short-Term Forward Earnings Per Share Growth Rate (EGRSF)" near line 1297.
See also: methbooks/rules/scoring/compute_eps12f.py, methbooks/rules/scoring/compute_eps12b.py (upstream inputs).
"""
from __future__ import annotations

import polars as pl


def compute_egrsf(df: pl.DataFrame) -> pl.DataFrame:
    assert "eps12f" in df.columns, f"eps12f column missing: {df.columns}"
    assert "eps12b" in df.columns, f"eps12b column missing: {df.columns}"
    out = df.with_columns(
        pl.when(
            pl.col("eps12b").is_not_null()
            & pl.col("eps12f").is_not_null()
            & (pl.col("eps12b") != 0)
        )
        .then((pl.col("eps12f") - pl.col("eps12b")) / pl.col("eps12b").abs())
        .otherwise(pl.lit(None).cast(pl.Float64))
        .alias("egrsf")
    )
    assert "egrsf" in out.columns, f"egrsf column missing after compute: {out.columns}"
    bad_zero = out.filter(
        (pl.col("eps12b") == 0) & pl.col("egrsf").is_not_null()
    ).height
    assert bad_zero == 0, f"egrsf not null for {bad_zero} rows where eps12b == 0"
    bad_null_b = out.filter(
        pl.col("eps12b").is_null() & pl.col("egrsf").is_not_null()
    ).height
    assert bad_null_b == 0, f"egrsf not null for {bad_null_b} rows where eps12b is null"
    return out
