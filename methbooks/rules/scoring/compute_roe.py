"""
Purpose: Compute return on equity as trailing 12-month EPS divided by latest BVPS; suppress result when book value is non-positive, the date gap between BV and earnings exceeds 18 months, BV date is newer than earnings date, or consolidation bases differ.
Datapoints: trailing_12m_earnings_per_share, book_value_per_share, earnings_period_date, book_value_period_date, consolidation_basis.
Thresholds: MAX_BV_EARNINGS_DATE_GAP_MONTHS = 18.
Source: methbooks/data/markdown/MSCI_Fundamental_Data_Methodology_20240625.md section "2.2.2 Return on Equity (ROE)" near line 1239.
See also: methbooks/rules/eligibility/exclude_stale_fundamental_data_18m.py (same 18-month gap concept at security level).
"""
from __future__ import annotations

import polars as pl

MAX_BV_EARNINGS_DATE_GAP_MONTHS = 18
_MAX_GAP_DAYS = MAX_BV_EARNINGS_DATE_GAP_MONTHS * 30.4375


def compute_roe(df: pl.DataFrame) -> pl.DataFrame:
    for col in ("trailing_12m_earnings_per_share", "book_value_per_share",
                "earnings_period_date", "book_value_period_date", "consolidation_basis"):
        assert col in df.columns, f"{col} column missing: {df.columns}"

    ep = pl.col("earnings_period_date").cast(pl.Date)
    bvp = pl.col("book_value_period_date").cast(pl.Date)
    gap_days = (ep - bvp).dt.total_days().abs()

    out = df.with_columns(
        pl.when(
            (pl.col("book_value_per_share") > 0)
            & (gap_days < _MAX_GAP_DAYS)
            & (bvp <= ep)
        )
        .then(pl.col("trailing_12m_earnings_per_share") / pl.col("book_value_per_share"))
        .otherwise(pl.lit(None).cast(pl.Float64))
        .alias("roe")
    )

    assert "roe" in out.columns, f"roe column missing after compute: {out.columns}"

    populated = out.filter(pl.col("roe").is_not_null())
    if populated.height > 0:
        bad_bv = populated.filter(pl.col("book_value_per_share") <= 0).height
        assert bad_bv == 0, f"roe populated for {bad_bv} rows with book_value_per_share <= 0"

        bad_gap = populated.filter(
            (pl.col("earnings_period_date").cast(pl.Date) - pl.col("book_value_period_date").cast(pl.Date)).dt.total_days().abs() >= _MAX_GAP_DAYS
        ).height
        assert bad_gap == 0, (
            f"roe populated for {bad_gap} rows with BV-earnings date gap >= {MAX_BV_EARNINGS_DATE_GAP_MONTHS} months"
        )

        bad_bvdate = populated.filter(
            pl.col("book_value_period_date").cast(pl.Date) > pl.col("earnings_period_date").cast(pl.Date)
        ).height
        assert bad_bvdate == 0, (
            f"roe populated for {bad_bvdate} rows where book_value_period_date > earnings_period_date"
        )

    suppressed_bv = out.filter(
        (pl.col("book_value_per_share") <= 0) & pl.col("roe").is_not_null()
    ).height
    assert suppressed_bv == 0, (
        f"roe not null for {suppressed_bv} rows with non-positive book_value_per_share"
    )
    return out
