"""
Purpose: Suppress EGRLF when the consensus estimate is an outlier (greater than 50% or less than -33%) and is based on only one analyst contributor.
Datapoints: egrlf_value, egrlf_contributor_count.
Thresholds: OUTLIER_MAX = 0.5, OUTLIER_MIN = -0.33, MAX_CONTRIBUTORS_FOR_OUTLIER_SUPPRESSION = 1.
Source: methbooks/data/markdown/MSCI_Fundamental_Data_Methodology_20240625.md section "2.2.6 Long-Term Forward Earnings Per Share Growth Rate (EGRLF)" near line 1305.
See also: methbooks/rules/scoring/compute_egrsf.py (uses EGRLF as an upstream input).
"""
from __future__ import annotations

import polars as pl

OUTLIER_MAX = 0.5
OUTLIER_MIN = -0.33
MAX_CONTRIBUTORS_FOR_OUTLIER_SUPPRESSION = 1


def exclude_egrlf_single_contributor_outlier(df: pl.DataFrame) -> pl.DataFrame:
    assert "egrlf_value" in df.columns, f"egrlf_value column missing: {df.columns}"
    assert "egrlf_contributor_count" in df.columns, (
        f"egrlf_contributor_count column missing: {df.columns}"
    )
    out = df.with_columns(
        pl.when(
            (pl.col("egrlf_value") > OUTLIER_MAX) | (pl.col("egrlf_value") < OUTLIER_MIN)
        )
        .then(
            pl.when(pl.col("egrlf_contributor_count") <= MAX_CONTRIBUTORS_FOR_OUTLIER_SUPPRESSION)
            .then(pl.lit(None).cast(pl.Float64))
            .otherwise(pl.col("egrlf_value"))
        )
        .otherwise(pl.col("egrlf_value"))
        .alias("egrlf_value")
    )
    assert "egrlf_value" in out.columns, f"egrlf_value column missing after suppress: {out.columns}"
    bad_high = out.filter(
        (pl.col("egrlf_value") > OUTLIER_MAX)
        & (pl.col("egrlf_contributor_count") <= MAX_CONTRIBUTORS_FOR_OUTLIER_SUPPRESSION)
        & pl.col("egrlf_value").is_not_null()
    ).height
    assert bad_high == 0, (
        f"egrlf_value not suppressed for {bad_high} outlier rows (> {OUTLIER_MAX}) with single contributor"
    )
    bad_low = out.filter(
        (pl.col("egrlf_value") < OUTLIER_MIN)
        & (pl.col("egrlf_contributor_count") <= MAX_CONTRIBUTORS_FOR_OUTLIER_SUPPRESSION)
        & pl.col("egrlf_value").is_not_null()
    ).height
    assert bad_low == 0, (
        f"egrlf_value not suppressed for {bad_low} outlier rows (< {OUTLIER_MIN}) with single contributor"
    )
    return out
