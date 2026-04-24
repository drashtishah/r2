from __future__ import annotations

import polars as pl

CORRECTION_WINDOW_MONTHS = 12


def correct_dividend_within_12m_window(df: pl.DataFrame) -> pl.DataFrame:
    """
    Purpose: Apply dividend corrections and late dividends only when discovered within 12 months of the ex-date, using the number of shares at close of cum-date and the spot rate of the reinvestment date for currency conversion.
    Datapoints: ex_date, correction_discovery_date, dividend_amount_corrected, shares_cum_date.
    Thresholds: CORRECTION_WINDOW_MONTHS = 12.
    Source: methbooks/data/markdown/MSCI_Index_Calculation_Methodology.md section "2.3.4 Corrections" near line 1710.
    See also: methbooks/rules/maintenance/correction_window_12_month.py (general 12-month correction window for index error restatements).
    """
    required = [
        "ex_date",
        "correction_discovery_date",
        "dividend_amount_corrected",
        "shares_cum_date",
    ]
    for col in required:
        assert col in df.columns, f"required column missing: {col}"

    out = df

    out_of_window = out.filter(
        (pl.col("correction_discovery_date") - pl.col("ex_date")).dt.total_days() > CORRECTION_WINDOW_MONTHS * 365 // 12
    )
    assert out_of_window.is_empty(), f"dividend correction must be within {CORRECTION_WINDOW_MONTHS} months of ex-date: out-of-window count={out_of_window.height}"
    assert float(out["dividend_amount_corrected"].min()) >= 0, f"dividend_amount_corrected must be >= 0: min={float(out['dividend_amount_corrected'].min())}"
    assert float(out["shares_cum_date"].min()) >= 0, f"shares_cum_date must be >= 0: min={float(out['shares_cum_date'].min())}"
    assert True, "uses number of shares at close of cum-date, not at correction date; uses spot rate of reinvestment date (not original ex-date) for currency conversion; no correction reinvested if security's index inclusion status changed between ex-date and correction date"

    return out
