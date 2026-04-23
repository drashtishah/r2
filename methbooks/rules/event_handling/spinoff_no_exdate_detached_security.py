# methbooks/rules/event_handling/spinoff_no_exdate_detached_security.py
"""
Purpose: When spun-off security does not trade on ex-date, create a detached security valued at the difference between cum and ex prices of the parent; maintain until spun-off starts trading; delete as of close of spun-off first trading day.
Datapoints: security_id, closing_price
Thresholds: MIN_ADVANCE_NOTIFICATION_BEFORE_DETACHED_DELETION_BUSINESS_DAYS = 2
Source: meth-pipeline/MSCI_Corporate_Events_Methodology_20260210/2026-04-23T13-31-41Z/input/markdown.md section "2.8.2 Spun-off not trading on the ex-date" near line 3138.
See also: methbooks/methodologies/MSCI/corporate_events.py (corporate events apply pipeline).
"""
from __future__ import annotations

import polars as pl

MIN_ADVANCE_NOTIFICATION_BEFORE_DETACHED_DELETION_BUSINESS_DAYS = 2


def spinoff_no_exdate_detached_security(df: pl.DataFrame) -> pl.DataFrame:
    required = ["security_id", "closing_price"]
    missing = [c for c in required if c not in df.columns]
    assert not missing, f"missing required columns: {missing}"

    out = df

    if "weight" in out.columns:
        weight_min = float(out["weight"].min())
        assert weight_min >= 0, (
            f"detached security weight must be >= 0, actual min: {weight_min}"
        )

    if "is_detached" in out.columns and "last_price_prior_to_event" in out.columns:
        detached = out.filter(pl.col("is_detached"))
        invalid = detached.filter(
            pl.col("closing_price") != pl.col("last_price_prior_to_event")
        )
        assert invalid.is_empty(), (
            f"detached security price must equal last price prior to event; "
            f"offending security_ids: {invalid['security_id'].to_list()}"
        )

    cp_min = float(out["closing_price"].min())
    assert cp_min >= 0, f"closing_price must be >= 0, actual min: {cp_min}"

    return out
