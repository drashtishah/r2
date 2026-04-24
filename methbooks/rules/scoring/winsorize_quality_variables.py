"""
Purpose: Clip extreme values of ROE, D/E, and Earnings Variability at 5th/95th percentile
    within each Parent Index.
Datapoints: roe, debt_to_equity, earnings_variability.
Thresholds: LOWER_PERCENTILE = 0.05, UPPER_PERCENTILE = 0.95.
Source: methbooks/data/markdown/msci_quality_indexes_methodology_20250520.md section
    "2.2.1 Winsorizing The Variable" near line 143.
"""
from __future__ import annotations

import polars as pl

LOWER_PERCENTILE = 0.05
UPPER_PERCENTILE = 0.95

_COLS = ["roe", "debt_to_equity", "earnings_variability"]


def winsorize_quality_variables(df: pl.DataFrame) -> pl.DataFrame:
    out = df
    p5_roe: float | None = None
    p95_roe: float | None = None

    for col in _COLS:
        non_null = df[col].drop_nulls()
        if non_null.is_empty():
            out = out.with_columns(pl.lit(None).cast(pl.Float64).alias(f"{col}_winsorized"))
            continue

        p5 = float(non_null.quantile(LOWER_PERCENTILE, interpolation="linear"))
        p95 = float(non_null.quantile(UPPER_PERCENTILE, interpolation="linear"))

        if col == "roe":
            p5_roe = p5
            p95_roe = p95

        out = out.with_columns(
            pl.when(pl.col(col).is_null())
            .then(pl.lit(None).cast(pl.Float64))
            .otherwise(pl.col(col).clip(p5, p95))
            .alias(f"{col}_winsorized")
        )

    assert "roe_winsorized" in out.columns, f"roe_winsorized missing: {out.columns}"
    assert "debt_to_equity_winsorized" in out.columns, (
        f"debt_to_equity_winsorized missing: {out.columns}"
    )
    assert "earnings_variability_winsorized" in out.columns, (
        f"earnings_variability_winsorized missing: {out.columns}"
    )

    if p5_roe is not None and p95_roe is not None:
        bad = out.filter(
            pl.col("roe_winsorized").is_not_null()
            & (
                (pl.col("roe_winsorized") < p5_roe - 1e-9)
                | (pl.col("roe_winsorized") > p95_roe + 1e-9)
            )
        )
        assert bad.height == 0, (
            f"roe_winsorized out of [{p5_roe}, {p95_roe}] for {bad.height} rows"
        )

    return out
