"""
Purpose: Compute 5-year historical EPS growth trend (EGRO) as OLS regression slope of annual EPS over cumulative months, divided by the mean of absolute EPS values; require at least four comparable annual EPS data points.
Datapoints: five_year_eps_history.
Thresholds: MIN_COMPARABLE_YEARS = 4.
Source: methbooks/data/markdown/MSCI_Fundamental_Data_Methodology_20240625.md section "2.2.1 Long-Term Historical Growth Trends" near line 1200.
See also: methbooks/rules/scoring/compute_sgro_ols.py (identical formula applied to SPS history).
"""
from __future__ import annotations

import polars as pl

MIN_COMPARABLE_YEARS = 4


def _ols_slope_over_mean_abs(values: list[float | None]) -> float | None:
    xs = [v for v in values if v is not None]
    if len(xs) < MIN_COMPARABLE_YEARS:
        return None
    n = len(xs)
    months = [(i + 1) * 12 for i in range(n)]
    mean_x = sum(months) / n
    mean_y = sum(xs) / n
    num = sum((months[i] - mean_x) * (xs[i] - mean_y) for i in range(n))
    den = sum((months[i] - mean_x) ** 2 for i in range(n))
    if den == 0:
        return None
    slope = num / den
    mean_abs = sum(abs(v) for v in xs) / n
    if mean_abs == 0:
        return None
    return slope / mean_abs


def compute_egro_ols(df: pl.DataFrame) -> pl.DataFrame:
    assert "five_year_eps_history" in df.columns, (
        f"five_year_eps_history column missing: {df.columns}"
    )
    egro_values = [
        _ols_slope_over_mean_abs(row) for row in df["five_year_eps_history"].to_list()
    ]
    out = df.with_columns(pl.Series("egro", egro_values, dtype=pl.Float64))
    assert "egro" in out.columns, f"egro column missing after compute: {out.columns}"
    populated = out.filter(pl.col("egro").is_not_null())
    for row in populated["five_year_eps_history"].to_list():
        non_null = [v for v in row if v is not None]
        assert len(non_null) >= MIN_COMPARABLE_YEARS, (
            f"egro populated for row with only {len(non_null)} non-null EPS values "
            f"(min {MIN_COMPARABLE_YEARS} required)"
        )
    null_rows = out.filter(pl.col("egro").is_null())
    for row in null_rows["five_year_eps_history"].to_list():
        non_null = [v for v in row if v is not None]
        assert len(non_null) < MIN_COMPARABLE_YEARS, (
            f"egro null but {len(non_null)} comparable EPS values present"
        )
    return out
