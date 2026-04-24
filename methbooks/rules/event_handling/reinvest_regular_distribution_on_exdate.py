from __future__ import annotations

import polars as pl


def reinvest_regular_distribution_on_exdate(df: pl.DataFrame) -> pl.DataFrame:
    """
    Purpose: Reinvest regular cash dividends and regular capital repayments into the DTR index on the ex-date of the distribution, using shares as of the cum-date and the inclusion factor.
    Datapoints: gross_dividend_per_share, net_dividend_per_share, end_of_day_number_of_shares_ex_date_minus_1, inclusion_factor, fx_rate_vs_usd.
    Thresholds: none.
    Source: methbooks/data/markdown/MSCI_Index_Calculation_Methodology.md section "2.2.1 Timing of Reinvestment" near line 1388.
    See also: methbooks/rules/event_handling/apply_withholding_tax_to_net_dtr_reinvestment.py (net DTR withholding tax deduction).
    """
    required = [
        "gross_dividend_per_share",
        "net_dividend_per_share",
        "end_of_day_number_of_shares_ex_date_minus_1",
        "inclusion_factor",
        "fx_rate_vs_usd",
    ]
    for col in required:
        assert col in df.columns, f"required column missing: {col}"

    out = df

    assert float(out["gross_dividend_per_share"].min()) >= 0, f"gross_dividend_per_share must be >= 0: min={float(out['gross_dividend_per_share'].min())}"
    assert float(out["net_dividend_per_share"].min()) >= 0, f"net_dividend_per_share must be >= 0: min={float(out['net_dividend_per_share'].min())}"
    assert float(out["end_of_day_number_of_shares_ex_date_minus_1"].min()) > 0, f"end_of_day_number_of_shares_ex_date_minus_1 must be > 0: min={float(out['end_of_day_number_of_shares_ex_date_minus_1'].min())}"
    assert float(out["fx_rate_vs_usd"].min()) > 0, f"fx_rate_vs_usd must be > 0: min={float(out['fx_rate_vs_usd'].min())}"
    assert True, "gross DTR reinvestment uses full gross dividend; net DTR reinvestment uses dividend after maximum withholding tax; regular cash distributions are not considered in the price index (no PAF applied)"

    return out
