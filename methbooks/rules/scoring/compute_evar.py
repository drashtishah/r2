"""
Purpose: Compute earnings variability as the standard deviation of year-on-year EPS growth over the last 5 fiscal years; requires at least 5 comparable EPS values; sign of denominator follows sign of prior-year EPS; growth null when prior-year EPS is zero.
Datapoints: five_year_eps_history.
Thresholds: MIN_COMPARABLE_EPS_PERIODS = 5.
Source: methbooks/data/markdown/MSCI_Fundamental_Data_Methodology_20240625.md section "2.2.7 Earnings Variability (EVAR)" near line 1383.
See also: methbooks/rules/scoring/compute_egro_ols.py (uses same five_year_eps_history, requires 4 not 5).
"""
from __future__ import annotations

import statistics

import polars as pl

MIN_COMPARABLE_EPS_PERIODS = 5


def _compute_evar(values: list[float | None]) -> float | None:
    xs = [v for v in values if v is not None]
    if len(xs) < MIN_COMPARABLE_EPS_PERIODS:
        return None
    growths: list[float] = []
    for i in range(1, len(xs)):
        prior = xs[i - 1]
        if prior == 0:
            continue
        g = (xs[i] - prior) / abs(prior)
        growths.append(g)
    if len(growths) < 2:
        return None
    return statistics.stdev(growths)


def compute_evar(df: pl.DataFrame) -> pl.DataFrame:
    assert "five_year_eps_history" in df.columns, (
        f"five_year_eps_history column missing: {df.columns}"
    )
    evar_values = [_compute_evar(row) for row in df["five_year_eps_history"].to_list()]
    out = df.with_columns(pl.Series("evar", evar_values, dtype=pl.Float64))
    assert "evar" in out.columns, f"evar column missing after compute: {out.columns}"
    populated = out.filter(pl.col("evar").is_not_null())
    for row in populated["five_year_eps_history"].to_list():
        non_null = [v for v in row if v is not None]
        assert len(non_null) >= MIN_COMPARABLE_EPS_PERIODS, (
            f"evar populated for row with only {len(non_null)} comparable EPS values "
            f"(min {MIN_COMPARABLE_EPS_PERIODS} required)"
        )
    null_rows = out.filter(pl.col("evar").is_null())
    for row in null_rows["five_year_eps_history"].to_list():
        non_null = [v for v in row if v is not None]
        assert len(non_null) < MIN_COMPARABLE_EPS_PERIODS, (
            f"evar null but {len(non_null)} comparable EPS values present"
        )
    return out
