"""
Purpose: Exclude per-share and ratio calculations when the gap between calculation date and financial reporting period end exceeds 18 months, preventing use of outdated fundamentals.
Datapoints: financial_period_end_date, calculation_date.
Thresholds: MAX_REPORTING_GAP_MONTHS = 18.
Source: methbooks/data/markdown/MSCI_Fundamental_Data_Methodology_20240625.md section "1.2 Historical Per Share Data Calculations" near line 685.
See also: methbooks/rules/scoring/compute_roe.py (applies same 18-month gap check for BV vs earnings dates).
"""
from __future__ import annotations

import polars as pl

MAX_REPORTING_GAP_MONTHS = 18


def exclude_stale_fundamental_data_18m(df: pl.DataFrame) -> pl.DataFrame:
    assert "financial_period_end_date" in df.columns, (
        f"financial_period_end_date column missing: {df.columns}"
    )
    assert "calculation_date" in df.columns, (
        f"calculation_date column missing: {df.columns}"
    )
    out = df.filter(
        (
            pl.col("calculation_date").cast(pl.Date) - pl.col("financial_period_end_date").cast(pl.Date)
        ).dt.total_days()
        < MAX_REPORTING_GAP_MONTHS * 30.4375
    )
    assert "financial_period_end_date" in out.columns, (
        f"financial_period_end_date column missing after filter: {out.columns}"
    )
    assert "calculation_date" in out.columns, (
        f"calculation_date column missing after filter: {out.columns}"
    )
    if out.height > 0:
        max_gap_days = float(
            (
                out["calculation_date"].cast(pl.Date) - out["financial_period_end_date"].cast(pl.Date)
            ).dt.total_days().max()
        )
        assert max_gap_days < MAX_REPORTING_GAP_MONTHS * 30.4375, (
            f"security with reporting gap >= {MAX_REPORTING_GAP_MONTHS} months survived: "
            f"max gap days={max_gap_days}"
        )
    return out
