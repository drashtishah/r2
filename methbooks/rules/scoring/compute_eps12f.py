"""
Purpose: Compute 12-month forward EPS on a rolling basis by blending current-year and next-year consensus forecasts weighted by months remaining in fiscal year; fall back to EPS1 alone when EPS2 is unavailable and M >= 8.
Datapoints: eps1_consensus_forecast, eps2_consensus_forecast, months_to_fiscal_year_end.
Thresholds: EPS2_UNAVAILABLE_MIN_M_FOR_FALLBACK = 8.
Source: methbooks/data/markdown/MSCI_Fundamental_Data_Methodology_20240625.md section "1.3 Forecasted Per Share Data" near line 891.
See also: methbooks/rules/scoring/compute_eps12b.py (mirrors this formula anchored on historical EPS0).
"""
from __future__ import annotations

import polars as pl

EPS2_UNAVAILABLE_MIN_M_FOR_FALLBACK = 8


def compute_eps12f(df: pl.DataFrame) -> pl.DataFrame:
    assert "eps1_consensus_forecast" in df.columns, (
        f"eps1_consensus_forecast column missing: {df.columns}"
    )
    assert "months_to_fiscal_year_end" in df.columns, (
        f"months_to_fiscal_year_end column missing: {df.columns}"
    )
    m_min = int(df["months_to_fiscal_year_end"].min())
    m_max = int(df["months_to_fiscal_year_end"].max())
    assert 0 <= m_min and m_max <= 12, (
        f"months_to_fiscal_year_end out of [0,12]: min={m_min}, max={m_max}"
    )
    m = pl.col("months_to_fiscal_year_end")
    eps1 = pl.col("eps1_consensus_forecast")
    eps2 = pl.col("eps2_consensus_forecast") if "eps2_consensus_forecast" in df.columns else pl.lit(None).cast(pl.Float64)

    out = df.with_columns(
        pl.when(eps2.is_null() & (m >= EPS2_UNAVAILABLE_MIN_M_FOR_FALLBACK))
        .then(eps1)
        .when(eps2.is_not_null())
        .then((m * eps1 + (12 - m) * eps2) / 12)
        .otherwise(pl.lit(None).cast(pl.Float64))
        .alias("eps12f")
    )
    assert "eps12f" in out.columns, f"eps12f column missing after compute: {out.columns}"
    fallback_rows = out.filter(
        pl.col("eps2_consensus_forecast").is_null()
        & (pl.col("months_to_fiscal_year_end") >= EPS2_UNAVAILABLE_MIN_M_FOR_FALLBACK)
        & pl.col("eps12f").is_not_null()
    )
    if fallback_rows.height > 0:
        mismatches = fallback_rows.filter(
            (pl.col("eps12f") - pl.col("eps1_consensus_forecast")).abs() > 1e-9
        ).height
        assert mismatches == 0, (
            f"eps12f != eps1 for {mismatches} fallback rows (eps2 null, M >= {EPS2_UNAVAILABLE_MIN_M_FOR_FALLBACK})"
        )
    return out
