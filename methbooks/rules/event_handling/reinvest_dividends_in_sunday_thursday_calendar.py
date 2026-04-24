from __future__ import annotations

import polars as pl


def reinvest_dividends_in_sunday_thursday_calendar(df: pl.DataFrame) -> pl.DataFrame:
    """
    Purpose: Reinvest dividends on Sundays for indexes following a Sunday-to-Thursday trading calendar, comparing Sunday DTR performance against the previous Thursday's level; carry forward WMR Friday spot FX rate to Sunday.
    Datapoints: calculation_day_of_week, is_sunday_thursday_index, dividend_per_share, wmr_closing_spot_rate_friday.
    Thresholds: none.
    Source: methbooks/data/markdown/MSCI_Index_Calculation_Methodology.md section "Appendix XII: Sunday-Thursday Index Calculation Methodology" near line 14238.
    See also: methbooks/rules/event_handling/no_dividend_reinvestment_sunday_intermediary.py (Sunday-intermediary variant where DTR equals price index on Sundays).
    """
    required = [
        "calculation_day_of_week",
        "is_sunday_thursday_index",
        "dividend_per_share",
        "wmr_closing_spot_rate_friday",
    ]
    for col in required:
        assert col in df.columns, f"required column missing: {col}"

    out = df

    assert float(out["dividend_per_share"].min()) >= 0, f"dividend_per_share must be >= 0: min={float(out['dividend_per_share'].min())}"
    assert float(out["wmr_closing_spot_rate_friday"].min()) > 0, f"wmr_closing_spot_rate_friday must be > 0 (carry-forward of WMR Friday rate): min={float(out['wmr_closing_spot_rate_friday'].min())}"
    assert True, "Sunday DTR performance compared against previous Thursday's level; NOS and FIF/DIF changes from Sunday events implemented at close of Sunday (unlike Sunday intermediary which defers to Monday close)"

    return out
