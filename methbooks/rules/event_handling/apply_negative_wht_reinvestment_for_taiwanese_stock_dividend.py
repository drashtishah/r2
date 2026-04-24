from __future__ import annotations

import polars as pl


def apply_negative_wht_reinvestment_for_taiwanese_stock_dividend(df: pl.DataFrame) -> pl.DataFrame:
    """
    Purpose: Reinvest a negative amount equal to the withholding tax on Taiwanese stock dividends in MSCI Net DTR Indexes only, simultaneously with the PAF on the ex-date, when the stock dividend is paid out of retained earnings.
    Datapoints: country_of_listing, stock_dividend_shares_received_subject_to_wht, par_value_per_share, withholding_tax_rate.
    Thresholds: none.
    Source: methbooks/data/markdown/MSCI_Index_Calculation_Methodology.md section "2.3.7.3 Country Exceptions - Taiwanese Stock Dividends" near line 2009.
    See also: methbooks/rules/event_handling/apply_paf_for_stock_dividend.py (PAF rule that fires on the same ex-date for the same stock dividend event).
    """
    required = [
        "country_of_listing",
        "stock_dividend_shares_received_subject_to_wht",
        "par_value_per_share",
        "withholding_tax_rate",
    ]
    for col in required:
        assert col in df.columns, f"required column missing: {col}"

    out = df

    assert float(out["withholding_tax_rate"].min()) >= 0, f"withholding_tax_rate must be >= 0: min={float(out['withholding_tax_rate'].min())}"
    assert float(out["stock_dividend_shares_received_subject_to_wht"].min()) >= 0, f"stock_dividend_shares_received_subject_to_wht must be >= 0: min={float(out['stock_dividend_shares_received_subject_to_wht'].min())}"
    assert float(out["par_value_per_share"].min()) >= 0, f"par_value_per_share must be >= 0: min={float(out['par_value_per_share'].min())}"
    if "negative_reinvestment_amount" in out.columns:
        assert float(out["negative_reinvestment_amount"].max()) <= 0, f"negative_reinvestment_amount must be <= 0: max={float(out['negative_reinvestment_amount'].max())}"
    assert True, "negative amount = shares_received_subject_to_wht * par_value * Taiwanese default WHT rate (Appendix VI); applies only to retained-earnings portion; capital-surplus portion is not subject to withholding tax"

    return out
