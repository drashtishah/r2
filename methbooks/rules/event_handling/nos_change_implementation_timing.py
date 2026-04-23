# methbooks/rules/event_handling/nos_change_implementation_timing.py
"""
Purpose: Implement NOS/FIF/DIF changes from primary equity offerings and debt-equity swaps as of close of first trading day of new shares; implement secondary offerings and block sales with two full business days advance notification; defer convertible instrument conversions to next Index Review.
Datapoints: security_id, nos, fif, dif, is_trading, event_type
Thresholds: MIN_ADVANCE_NOTIFICATION_BUSINESS_DAYS = 2
Source: meth-pipeline/MSCI_Corporate_Events_Methodology_20260210/2026-04-23T13-31-41Z/input/markdown.md section "4.1 Implementation Timing" near line 4691.
See also: methbooks/methodologies/MSCI/corporate_events.py (corporate events apply pipeline).
"""
from __future__ import annotations

import polars as pl

MIN_ADVANCE_NOTIFICATION_BUSINESS_DAYS = 2

_DEFERRED_EVENT_TYPES = {"convertible_bond_conversion", "debt_equity_swap_strategic"}


def nos_change_implementation_timing(df: pl.DataFrame) -> pl.DataFrame:
    required = ["security_id", "nos", "fif", "dif", "is_trading", "event_type"]
    missing = [c for c in required if c not in df.columns]
    assert not missing, f"missing required columns: {missing}"

    out = df

    nos_min = float(out["nos"].min())
    assert nos_min >= 0, f"nos must be >= 0, actual min: {nos_min}"

    assert MIN_ADVANCE_NOTIFICATION_BUSINESS_DAYS >= 2, (
        f"secondary offerings and block sales require >= 2 business days advance notification, "
        f"actual: {MIN_ADVANCE_NOTIFICATION_BUSINESS_DAYS}"
    )

    # If security does not trade on effective date, event implemented next trading day.
    if "effective_date_is_trading" in out.columns and "implementation_deferred_one_day" in out.columns:
        not_trading = out.filter(~pl.col("effective_date_is_trading"))
        not_deferred = not_trading.filter(~pl.col("implementation_deferred_one_day").cast(pl.Boolean))
        assert not_deferred.is_empty(), (
            f"if security does not trade on effective date, event implemented next trading day; "
            f"offending security_ids: {not_deferred['security_id'].to_list()}"
        )

    # Convertible bond and debt-equity swap conversions deferred to next Index Review.
    if "deferred_to_index_review" in out.columns:
        deferred_types = out.filter(pl.col("event_type").is_in(list(_DEFERRED_EVENT_TYPES)))
        not_deferred = deferred_types.filter(~pl.col("deferred_to_index_review").cast(pl.Boolean))
        assert not_deferred.is_empty(), (
            f"convertible bond and debt-equity swap conversions must be deferred to Index Review; "
            f"offending security_ids: {not_deferred['security_id'].to_list()}"
        )

    return out
