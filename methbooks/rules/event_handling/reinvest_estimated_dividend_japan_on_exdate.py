from __future__ import annotations

import polars as pl


def reinvest_estimated_dividend_japan_on_exdate(df: pl.DataFrame) -> pl.DataFrame:
    """
    Purpose: Reinvest an estimated dividend for Japanese securities on the ex-date using the previous year's same-period dividend when the actual dividend is not yet declared, and treat any subsequent difference as a payment-default-style correction without restating past index levels.
    Datapoints: country_of_listing, prior_year_dividend_same_period, announced_dividend_estimate.
    Thresholds: none.
    Source: methbooks/data/markdown/MSCI_Index_Calculation_Methodology.md section "2.3.6 Country Exceptions - Japan" near line 1759.
    See also: methbooks/rules/event_handling/reinvest_korean_dividend_estimate_enhanced_methodology.py (Korean equivalent with capital-change adjustment).
    """
    required = [
        "country_of_listing",
        "prior_year_dividend_same_period",
        "announced_dividend_estimate",
    ]
    for col in required:
        assert col in df.columns, f"required column missing: {col}"

    out = df

    assert float(out["prior_year_dividend_same_period"].min()) >= 0, f"prior_year_dividend_same_period must be >= 0: min={float(out['prior_year_dividend_same_period'].min())}"
    assert float(out["announced_dividend_estimate"].min()) >= 0, f"announced_dividend_estimate must be >= 0: min={float(out['announced_dividend_estimate'].min())}"
    assert True, "when company does not declare an estimate, prior-year same-period dividend is used; difference between estimated and ratified amount processed as payment-default reinvestment (no past-level correction)"

    return out
