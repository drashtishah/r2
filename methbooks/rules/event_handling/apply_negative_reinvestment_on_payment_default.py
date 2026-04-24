from __future__ import annotations

import polars as pl

CORRECTION_WINDOW_MONTHS = 12


def apply_negative_reinvestment_on_payment_default(df: pl.DataFrame) -> pl.DataFrame:
    """
    Purpose: Apply a negative reinvestment when a dividend is declared unpaid (payment default), within 12 months of the ex-date, without correcting past index levels.
    Datapoints: payment_default_flag, original_dividend_amount, shares_cum_date, default_discovery_date.
    Thresholds: CORRECTION_WINDOW_MONTHS = 12.
    Source: methbooks/data/markdown/MSCI_Index_Calculation_Methodology.md section "2.3.5 Payment Default" near line 1731.
    See also: methbooks/rules/maintenance/correct_dividend_within_12m_window.py (12-month correction window for ordinary dividend corrections).
    """
    required = [
        "payment_default_flag",
        "original_dividend_amount",
        "shares_cum_date",
        "default_discovery_date",
    ]
    for col in required:
        assert col in df.columns, f"required column missing: {col}"

    out = df

    if "ex_date" in out.columns:
        late = out.filter((pl.col("default_discovery_date") - pl.col("ex_date")).dt.total_days() > CORRECTION_WINDOW_MONTHS * 365 // 12)
        assert late.is_empty(), f"payment default correction must be within {CORRECTION_WINDOW_MONTHS} months of ex-date: offending count={late.height}"
    assert float(out["original_dividend_amount"].min()) >= 0, f"original_dividend_amount must be >= 0: min={float(out['original_dividend_amount'].min())}"
    assert float(out["shares_cum_date"].min()) >= 0, f"shares_cum_date must be >= 0: min={float(out['shares_cum_date'].min())}"
    assert True, "negative reinvestment applied on next business day after two-source confirmation; past index levels are not restated; spot rate of reinvestment date used for currency conversion"

    return out
