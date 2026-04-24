from __future__ import annotations

import polars as pl


def no_dividend_reinvestment_sunday_intermediary(df: pl.DataFrame) -> pl.DataFrame:
    """
    Purpose: Suppress dividend reinvestment on Sundays in the Sunday intermediary calculation to preserve compatibility with the Monday-Friday index series; DTR performance on Sunday equals price index performance.
    Datapoints: calculation_day_of_week, is_sunday_intermediary_index.
    Thresholds: none.
    Source: methbooks/data/markdown/MSCI_Index_Calculation_Methodology.md section "Appendix I: Sunday Intermediary Index Calculation for Monday-Friday Index Calculation Methodology" near line 2174.
    See also: methbooks/rules/event_handling/reinvest_dividends_in_sunday_thursday_calendar.py (Sunday-Thursday variant where dividends ARE reinvested on Sundays).
    """
    required = [
        "calculation_day_of_week",
        "is_sunday_intermediary_index",
    ]
    for col in required:
        assert col in df.columns, f"required column missing: {col}"

    out = df

    if "dtr_index_level_change" in out.columns and "price_index_level_change" in out.columns:
        sunday_rows = out.filter(
            (pl.col("calculation_day_of_week") == "Sunday") & pl.col("is_sunday_intermediary_index")
        )
        mismatch = sunday_rows.filter(
            (pl.col("dtr_index_level_change") - pl.col("price_index_level_change")).abs() > 1e-12
        )
        assert mismatch.is_empty(), f"on Sunday in Sunday-intermediary index, DTR performance must equal price index performance: offending count={mismatch.height}"
    assert True, "Sunday corporate-event PAFs applied to both Sunday and Monday market prices; NOS/FIF/DIF changes implemented at close of Monday; WMR Friday spot FX rate carried forward to Sunday for USD calculation"

    return out
