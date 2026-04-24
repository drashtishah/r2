"""
Purpose: Compute 12-month backward EPS by blending last reported fiscal EPS with the current-year consensus estimate, mirroring the EPS12F formula but anchored on historical results.
Datapoints: eps0_last_fiscal_year_eps, eps1_consensus_forecast, months_to_fiscal_year_end.
Thresholds: none.
Source: methbooks/data/markdown/MSCI_Fundamental_Data_Methodology_20240625.md section "1.3 Forecasted Per Share Data" near line 969.
See also: methbooks/rules/scoring/compute_eps12f.py (forward counterpart; same blending formula).
"""
from __future__ import annotations

import polars as pl


def compute_eps12b(df: pl.DataFrame) -> pl.DataFrame:
    assert "eps0_last_fiscal_year_eps" in df.columns, (
        f"eps0_last_fiscal_year_eps column missing: {df.columns}"
    )
    assert "eps1_consensus_forecast" in df.columns, (
        f"eps1_consensus_forecast column missing: {df.columns}"
    )
    assert "months_to_fiscal_year_end" in df.columns, (
        f"months_to_fiscal_year_end column missing: {df.columns}"
    )
    m = pl.col("months_to_fiscal_year_end")
    eps0 = pl.col("eps0_last_fiscal_year_eps")
    eps1 = pl.col("eps1_consensus_forecast")
    out = df.with_columns(
        pl.when(eps0.is_not_null() & eps1.is_not_null())
        .then((m * eps0 + (12 - m) * eps1) / 12)
        .otherwise(pl.lit(None).cast(pl.Float64))
        .alias("eps12b")
    )
    assert "eps12b" in out.columns, f"eps12b column missing after compute: {out.columns}"
    populated = out.filter(pl.col("eps12b").is_not_null())
    if populated.height > 0:
        eps0_missing = populated.filter(pl.col("eps0_last_fiscal_year_eps").is_null()).height
        assert eps0_missing == 0, (
            f"eps12b populated but eps0_last_fiscal_year_eps null for {eps0_missing} rows"
        )
    return out
