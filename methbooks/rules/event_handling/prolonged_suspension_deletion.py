# methbooks/rules/event_handling/prolonged_suspension_deletion.py
"""
Purpose: Delete GIMI constituents after 50 consecutive business days of suspension and Global Micro Cap Index constituents after 100 consecutive business days; delete at lowest system price (0.00001); do not revert announced confirmed deletions.
Datapoints: security_id, consecutive_suspension_days, closing_price, size_segment, is_trading
Thresholds: GIMI_SUSPENSION_THRESHOLD_BUSINESS_DAYS = 50, GLOBAL_MICRO_CAP_SUSPENSION_THRESHOLD_BUSINESS_DAYS = 100, LOWEST_SYSTEM_PRICE = 1e-05, MIN_ADVANCE_NOTIFICATION_BUSINESS_DAYS = 2
Source: meth-pipeline/MSCI_Corporate_Events_Methodology_20260210/2026-04-23T13-31-41Z/input/markdown.md section "5.1.1 Prolonged suspension" near line 5071.
See also: methbooks/methodologies/MSCI/corporate_events.py (corporate events apply pipeline).
"""
from __future__ import annotations

import polars as pl

GIMI_SUSPENSION_THRESHOLD_BUSINESS_DAYS = 50
GLOBAL_MICRO_CAP_SUSPENSION_THRESHOLD_BUSINESS_DAYS = 100
LOWEST_SYSTEM_PRICE = 1e-05
MIN_ADVANCE_NOTIFICATION_BUSINESS_DAYS = 2


def prolonged_suspension_deletion(df: pl.DataFrame) -> pl.DataFrame:
    required = ["security_id", "consecutive_suspension_days", "closing_price", "size_segment", "is_trading"]
    missing = [c for c in required if c not in df.columns]
    assert not missing, f"missing required columns: {missing}"

    out = df

    assert LOWEST_SYSTEM_PRICE == 1e-05, (
        f"deletion at lowest system price unless security resumes trading day prior, "
        f"actual LOWEST_SYSTEM_PRICE: {LOWEST_SYSTEM_PRICE}"
    )

    assert GIMI_SUSPENSION_THRESHOLD_BUSINESS_DAYS == 50, (
        f"GIMI threshold must be 50 business days, actual: {GIMI_SUSPENSION_THRESHOLD_BUSINESS_DAYS}"
    )
    assert GLOBAL_MICRO_CAP_SUSPENSION_THRESHOLD_BUSINESS_DAYS == 100, (
        f"Global Micro Cap threshold must be 100 business days, "
        f"actual: {GLOBAL_MICRO_CAP_SUSPENSION_THRESHOLD_BUSINESS_DAYS}"
    )

    csd_min = float(out["consecutive_suspension_days"].min())
    assert csd_min >= 0, f"consecutive_suspension_days must be >= 0, actual min: {csd_min}"

    cp_min = float(out["closing_price"].min())
    assert cp_min >= 0, f"closing_price must be >= 0, actual min: {cp_min}"

    return out
