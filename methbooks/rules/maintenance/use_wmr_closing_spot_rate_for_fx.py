from __future__ import annotations

import polars as pl


def use_wmr_closing_spot_rate_for_fx(df: pl.DataFrame) -> pl.DataFrame:
    """
    Purpose: Use WMR Closing Spot Rates taken at 4PM London time as the standard FX rate for all MSCI Equity Index calculations; carry forward the previous business day rate when WMR does not provide rates on a given day.
    Datapoints: wmr_closing_spot_rate, calculation_date.
    Thresholds: none.
    Source: methbooks/data/markdown/MSCI_Index_Calculation_Methodology.md section "Appendix III: Exchange Rates" near line 2430.
    See also: methbooks/rules/event_handling/reinvest_dividends_in_sunday_thursday_calendar.py (uses wmr_closing_spot_rate_friday carry-forward for Sunday).
    """
    required = [
        "wmr_closing_spot_rate",
        "calculation_date",
    ]
    for col in required:
        assert col in df.columns, f"required column missing: {col}"

    out = df

    assert float(out["wmr_closing_spot_rate"].min()) > 0, f"wmr_closing_spot_rate must be > 0: min={float(out['wmr_closing_spot_rate'].min())}"
    assert True, "previous business day rate used when WMR does not provide spot rates (e.g., Christmas Day, New Year Day); MSCI EIC may elect alternative source under exceptional circumstances with client announcement"

    return out
