"""
Purpose: Prevent securities exhibiting extreme price increase (excess return breaching
any monitoring-period threshold) from being added to the Standard Indexes; they remain
in the IMI and are re-evaluated at the next review.
Datapoints: excess_return_5d_pct, excess_return_10d_pct, excess_return_15d_pct,
  excess_return_20d_pct, excess_return_25d_pct, excess_return_30d_pct,
  excess_return_35d_pct, excess_return_40d_pct, excess_return_45d_pct,
  excess_return_50d_pct, excess_return_55d_pct, excess_return_60d_pct,
  excess_return_90d_pct, excess_return_120d_pct, excess_return_150d_pct,
  excess_return_180d_pct, excess_return_250d_pct,
  is_standard_index_addition_candidate.
Thresholds: THRESHOLD_5D_PCT=100, THRESHOLD_10D_PCT=100, THRESHOLD_15D_PCT=100,
  THRESHOLD_20D_PCT=100, THRESHOLD_25D_PCT=200, THRESHOLD_30D_PCT=200,
  THRESHOLD_35D_PCT=200, THRESHOLD_40D_PCT=200, THRESHOLD_45D_PCT=400,
  THRESHOLD_50D_PCT=400, THRESHOLD_55D_PCT=400, THRESHOLD_60D_PCT=400,
  THRESHOLD_90D_PCT=500, THRESHOLD_120D_PCT=800, THRESHOLD_150D_PCT=1500,
  THRESHOLD_180D_PCT=1500, THRESHOLD_250D_PCT=2500.
Source: meth-pipeline/MSCI_Global_Investable_Market_Indexes_Methodology_20260331/2026-04-24T07-46-35Z/input/markdown.md section "2.3.6.3 Treatment of Securities that Exhibit Extreme Price Increase" near line 1661.
See also: methbooks/rules/selection/apply_standard_index_minimum_constituents.py (minimum constituent override).
"""
from __future__ import annotations

import polars as pl

_PERIOD_THRESHOLDS: list[tuple[str, float]] = [
    ("excess_return_5d_pct", 100.0),
    ("excess_return_10d_pct", 100.0),
    ("excess_return_15d_pct", 100.0),
    ("excess_return_20d_pct", 100.0),
    ("excess_return_25d_pct", 200.0),
    ("excess_return_30d_pct", 200.0),
    ("excess_return_35d_pct", 200.0),
    ("excess_return_40d_pct", 200.0),
    ("excess_return_45d_pct", 400.0),
    ("excess_return_50d_pct", 400.0),
    ("excess_return_55d_pct", 400.0),
    ("excess_return_60d_pct", 400.0),
    ("excess_return_90d_pct", 500.0),
    ("excess_return_120d_pct", 800.0),
    ("excess_return_150d_pct", 1500.0),
    ("excess_return_180d_pct", 1500.0),
    ("excess_return_250d_pct", 2500.0),
]


def _breaches_any_threshold(df: pl.DataFrame) -> pl.Expr:
    expr = pl.lit(False)
    for col, threshold in _PERIOD_THRESHOLDS:
        if col in df.columns:
            expr = expr | (pl.col(col) > threshold)
    return expr


def apply_extreme_price_increase_standard_index_block(
    df: pl.DataFrame,
) -> pl.DataFrame:
    for col, _ in _PERIOD_THRESHOLDS:
        assert col in df.columns, f"{col} column missing: {df.columns}"
    assert "is_standard_index_addition_candidate" in df.columns, (
        f"is_standard_index_addition_candidate column missing: {df.columns}"
    )
    breaches = _breaches_any_threshold(df)
    # Block only Standard Index addition candidates that breach a threshold.
    out = df.with_columns(
        pl.when(pl.col("is_standard_index_addition_candidate") & breaches)
        .then(pl.lit(False))
        .otherwise(pl.col("is_standard_index_addition_candidate"))
        .alias("is_standard_index_addition_candidate")
    )
    # Technical assert: no addition candidate still has a breaching excess return.
    bad = out.filter(
        pl.col("is_standard_index_addition_candidate") & breaches
    )
    assert bad.height == 0, (
        f"Standard Index addition candidate breaching extreme price increase threshold"
        f" survived: {bad.height} rows"
    )
    return out
