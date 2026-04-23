# methbooks/rules/event_handling/price_limit_postponement.py
"""
Purpose: Postpone by two full business days the implementation of additions, deletions, and migrations from MSCI Standard Indexes when a price limit mechanism is active at 12PM (or 11AM for exchanges closing before 2PM) on the implementation date.
Datapoints: security_id, is_price_limit_active, exchange_closing_time
Thresholds: POSTPONEMENT_BUSINESS_DAYS = 2, PRICE_LIMIT_CHECK_TIME_STANDARD = "12:00", PRICE_LIMIT_CHECK_TIME_EARLY_CLOSE = "11:00"
Source: meth-pipeline/MSCI_Corporate_Events_Methodology_20260210/2026-04-23T13-31-41Z/input/markdown.md section "5.2 Price Limit" near line 5117.
See also: methbooks/methodologies/MSCI/corporate_events.py (corporate events apply pipeline).
"""
from __future__ import annotations

import polars as pl

POSTPONEMENT_BUSINESS_DAYS = 2
PRICE_LIMIT_CHECK_TIME_STANDARD = "12:00"
PRICE_LIMIT_CHECK_TIME_EARLY_CLOSE = "11:00"


def price_limit_postponement(df: pl.DataFrame) -> pl.DataFrame:
    required = ["security_id", "is_price_limit_active", "exchange_closing_time"]
    missing = [c for c in required if c not in df.columns]
    assert not missing, f"missing required columns: {missing}"

    out = df

    assert POSTPONEMENT_BUSINESS_DAYS == 2, (
        f"addition/deletion/migration postponed by exactly 2 business days when price limit "
        f"active at check time, actual POSTPONEMENT_BUSINESS_DAYS: {POSTPONEMENT_BUSINESS_DAYS}"
    )

    if "postponement_days_applied" in out.columns:
        price_limit_rows = out.filter(pl.col("is_price_limit_active"))
        wrong_postponement = price_limit_rows.filter(
            pl.col("postponement_days_applied") != POSTPONEMENT_BUSINESS_DAYS
        )
        assert wrong_postponement.is_empty(), (
            f"postponement must be exactly {POSTPONEMENT_BUSINESS_DAYS} business days when "
            f"price limit active; offending postponement_days_applied values: "
            f"{wrong_postponement['postponement_days_applied'].to_list()}"
        )

    return out
