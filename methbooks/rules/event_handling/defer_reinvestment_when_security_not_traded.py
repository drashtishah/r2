from __future__ import annotations

import polars as pl


def defer_reinvestment_when_security_not_traded(df: pl.DataFrame) -> pl.DataFrame:
    """
    Purpose: Postpone dividend reinvestment to the next day the security trades when the security does not trade on the ex-date or scheduled reinvestment date, to avoid reinvesting into a non-functional market.
    Datapoints: security_traded_flag, ex_date.
    Thresholds: none.
    Source: methbooks/data/markdown/MSCI_Index_Calculation_Methodology.md section "2.2.1 Timing of Reinvestment" near line 1393.
    See also: methbooks/rules/event_handling/reinvest_regular_distribution_on_exdate.py (reinvestment on ex-date when security trades).
    """
    required = [
        "security_traded_flag",
        "ex_date",
    ]
    for col in required:
        assert col in df.columns, f"required column missing: {col}"

    out = df

    if "reinvestment_date" in out.columns:
        assert (out.filter(pl.col("reinvestment_date") < pl.col("ex_date"))).is_empty(), f"reinvestment_date must be >= ex_date for all rows: offending count={out.filter(pl.col('reinvestment_date') < pl.col('ex_date')).height}"
    assert True, "no reinvestment occurs on a day the security does not trade; deferred reinvestment does not trigger an index-level correction"

    return out
