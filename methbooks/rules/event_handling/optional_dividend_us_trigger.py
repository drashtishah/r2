# methbooks/rules/event_handling/optional_dividend_us_trigger.py
"""
Purpose: For US optional dividends, apply PAF on ex-date for cash+shares component if the total dividend is >= 5% of market price; apply PAF for shares component only when < 5%.
Datapoints: security_id, closing_price, special_dividend_amount, country_of_classification
Thresholds: US_OPTIONAL_DIVIDEND_PAF_TRIGGER_PCT = 5
Source: meth-pipeline/MSCI_Corporate_Events_Methodology_20260210/2026-04-23T13-31-41Z/input/markdown.md section "3.5 Optional Dividends" near line 4053.
See also: methbooks/methodologies/MSCI/corporate_events.py (corporate events apply pipeline).
"""
from __future__ import annotations

import polars as pl

US_OPTIONAL_DIVIDEND_PAF_TRIGGER_PCT = 5

_US_COUNTRY_CODE = "US"


def optional_dividend_us_trigger(df: pl.DataFrame) -> pl.DataFrame:
    required = ["security_id", "closing_price", "special_dividend_amount", "country_of_classification"]
    missing = [c for c in required if c not in df.columns]
    assert not missing, f"missing required columns: {missing}"

    out = df

    assert "country_of_classification" in out.columns, (
        f"rule applies only when country_of_classification == US; "
        f"country_of_classification column must be present"
    )

    cp_min = float(out["closing_price"].min())
    assert cp_min > 0, f"closing_price must be > 0, actual min: {cp_min}"

    us_rows = out.filter(pl.col("country_of_classification") == _US_COUNTRY_CODE)
    if us_rows.is_empty():
        return out

    sd_min = float(us_rows["special_dividend_amount"].min())
    assert sd_min >= 0, f"special_dividend_amount must be >= 0 for US rows, actual min: {sd_min}"

    return out
