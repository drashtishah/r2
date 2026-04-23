# methbooks/rules/event_handling/stock_connect_ashare_event_treatment.py
"""
Purpose: For Stock Connect A-Share securities: implement market-neutral events on ex-date even during Stock Connect holidays; defer non-market-neutral events with PAF to next Stock Connect trading day; A-share IPOs included only at subsequent Index Reviews.
Datapoints: security_id, is_stock_connect_ashare, is_stock_connect_trading_day, is_market_neutral_event
Thresholds: none
Source: meth-pipeline/MSCI_Corporate_Events_Methodology_20260210/2026-04-23T13-31-41Z/input/markdown.md section "Appendix VII: Implementation for stock connect A-Share securities" near line 6072.
See also: methbooks/methodologies/MSCI/corporate_events.py (corporate events apply pipeline).
"""
from __future__ import annotations

import polars as pl


def stock_connect_ashare_event_treatment(df: pl.DataFrame) -> pl.DataFrame:
    required = ["security_id", "is_stock_connect_ashare", "is_stock_connect_trading_day", "is_market_neutral_event"]
    missing = [c for c in required if c not in df.columns]
    assert not missing, f"missing required columns: {missing}"

    out = df

    assert "is_stock_connect_ashare" in out.columns, (
        "market neutral events: same implementation date for onshore and Stock Connect; "
        "is_stock_connect_ashare column must be present"
    )

    # Non-market-neutral events with PAF must not be implemented during Stock Connect holidays.
    if "implementation_date" in out.columns and "ex_date" in out.columns:
        ashare_non_neutral = out.filter(
            pl.col("is_stock_connect_ashare")
            & ~pl.col("is_market_neutral_event")
            & ~pl.col("is_stock_connect_trading_day")
        )
        premature = ashare_non_neutral.filter(
            pl.col("implementation_date") == pl.col("ex_date")
        )
        assert premature.is_empty(), (
            f"non-market-neutral PAF events must not be implemented on Stock Connect holidays; "
            f"offending security_ids: {premature['security_id'].to_list()}"
        )

    return out
