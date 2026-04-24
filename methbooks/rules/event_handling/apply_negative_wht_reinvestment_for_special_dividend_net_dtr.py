from __future__ import annotations

import polars as pl

PAF_TRIGGER_THRESHOLD = 0.05


def apply_negative_wht_reinvestment_for_special_dividend_net_dtr(df: pl.DataFrame) -> pl.DataFrame:
    """
    Purpose: Reinvest a negative amount equal to the withholding tax on a 5%+ special cash dividend in MSCI Net DTR Indexes only, simultaneously with the PAF on the ex-date, so net DTR reflects the after-tax dividend impact.
    Datapoints: special_dividend_per_share, withholding_tax_rate, end_of_day_number_of_shares_ex_date_minus_1, inclusion_factor, fx_rate_vs_usd.
    Thresholds: PAF_TRIGGER_THRESHOLD = 0.05.
    Source: methbooks/data/markdown/MSCI_Index_Calculation_Methodology.md section "2.2.4 Dividends Resulting in a Reinvestment or in a Price Adjustment" near line 1607.
    See also: methbooks/rules/event_handling/apply_paf_for_special_dividend_gte_5pct.py (PAF rule that fires on the same ex-date).
    """
    required = [
        "special_dividend_per_share",
        "withholding_tax_rate",
        "end_of_day_number_of_shares_ex_date_minus_1",
        "inclusion_factor",
        "fx_rate_vs_usd",
    ]
    for col in required:
        assert col in df.columns, f"required column missing: {col}"

    out = df

    assert float(out["withholding_tax_rate"].min()) >= 0, f"withholding_tax_rate must be >= 0: min={float(out['withholding_tax_rate'].min())}"
    assert float(out["withholding_tax_rate"].max()) <= 1.0, f"withholding_tax_rate must be <= 1: max={float(out['withholding_tax_rate'].max())}"
    assert float(out["special_dividend_per_share"].min()) >= 0, f"special_dividend_per_share must be >= 0: min={float(out['special_dividend_per_share'].min())}"
    if "negative_reinvestment_amount" in out.columns:
        assert float(out["negative_reinvestment_amount"].max()) <= 0, f"negative_reinvestment_amount must be <= 0 for all rows: max={float(out['negative_reinvestment_amount'].max())}"
    assert True, "negative reinvestment applies only to MSCI Net DTR Indexes; gross DTR and price index see only the PAF; fires on same ex-date as apply_paf_for_special_dividend_gte_5pct"

    return out
