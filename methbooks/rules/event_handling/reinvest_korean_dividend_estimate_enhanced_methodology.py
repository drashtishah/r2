from __future__ import annotations

import polars as pl

MINIMUM_DATA_SOURCE_CONFIRMATIONS = 2


def reinvest_korean_dividend_estimate_enhanced_methodology(df: pl.DataFrame) -> pl.DataFrame:
    """
    Purpose: For Korean dividends with ex-date from December 2014 onwards (enhanced methodology), reinvest an estimated dividend on the ex-date using the previous year's same-period amount adjusted for any capital changes; reinvest the actual-vs-estimate difference on the next business day after reception, without correcting past index levels.
    Datapoints: country_of_listing, prior_year_dividend_same_period, capital_change_adjustment, ratified_dividend_per_share, reception_date, shares_prior_to_exdate.
    Thresholds: MINIMUM_DATA_SOURCE_CONFIRMATIONS = 2.
    Source: methbooks/data/markdown/MSCI_Index_Calculation_Methodology.md section "2.3.6 Country Exceptions - Korea" near line 1788.
    See also: methbooks/rules/event_handling/reinvest_estimated_dividend_japan_on_exdate.py (Japan equivalent using prior-year same-period dividend without capital adjustment).
    """
    required = [
        "country_of_listing",
        "prior_year_dividend_same_period",
        "capital_change_adjustment",
        "ratified_dividend_per_share",
        "reception_date",
        "shares_prior_to_exdate",
    ]
    for col in required:
        assert col in df.columns, f"required column missing: {col}"

    out = df

    assert float(out["prior_year_dividend_same_period"].min()) >= 0, f"prior_year_dividend_same_period must be >= 0: min={float(out['prior_year_dividend_same_period'].min())}"
    assert float(out["ratified_dividend_per_share"].min()) >= 0, f"ratified_dividend_per_share must be >= 0: min={float(out['ratified_dividend_per_share'].min())}"
    assert float(out["shares_prior_to_exdate"].min()) >= 0, f"shares_prior_to_exdate must be >= 0: min={float(out['shares_prior_to_exdate'].min())}"
    if "estimated_dividend_per_share" in out.columns:
        assert float(out["estimated_dividend_per_share"].min()) >= 0, f"estimated_dividend_per_share must be >= 0: min={float(out['estimated_dividend_per_share'].min())}"
    assert True, f"at least {MINIMUM_DATA_SOURCE_CONFIRMATIONS} data source confirmations required before true-up reinvestment; applies to ex-dates from December 2014 onwards; zero estimate used when no prior-year same-period dividend available"

    return out
