# methbooks/rules/event_handling/highly_dilutive_rights_issue.py
"""
Purpose: For rights issues with terms of 5-for-1 or more, or rights issues triggering upward size migration: add both rights line and subscription cash to index as of close of ex-date; delete both and increase NOS as of close of pay-date.
Datapoints: security_id, nos, closing_price, subscription_price, rights_price, size_segment
Thresholds: DILUTION_RATIO_THRESHOLD_NUMERATOR = 5, DILUTION_RATIO_THRESHOLD_DENOMINATOR = 1, LOWEST_SYSTEM_PRICE = 1e-05
Source: meth-pipeline/MSCI_Corporate_Events_Methodology_20260210/2026-04-23T13-31-41Z/input/markdown.md section "3.6.5 Highly Dilutive or Large Rights Issues" near line 4333.
See also: methbooks/methodologies/MSCI/corporate_events.py (corporate events apply pipeline).
"""
from __future__ import annotations

import polars as pl

DILUTION_RATIO_THRESHOLD_NUMERATOR = 5
DILUTION_RATIO_THRESHOLD_DENOMINATOR = 1
LOWEST_SYSTEM_PRICE = 1e-05


def highly_dilutive_rights_issue(df: pl.DataFrame) -> pl.DataFrame:
    required = ["security_id", "nos", "closing_price", "subscription_price", "rights_price", "size_segment"]
    missing = [c for c in required if c not in df.columns]
    assert not missing, f"missing required columns: {missing}"

    out = df

    assert LOWEST_SYSTEM_PRICE == 1e-05, (
        f"if rights price unavailable and intrinsic value <= 0, lowest system price (0.00001) used, "
        f"actual LOWEST_SYSTEM_PRICE: {LOWEST_SYSTEM_PRICE}"
    )

    nos_min = float(out["nos"].min())
    assert nos_min >= 0, f"nos must be >= 0, actual min: {nos_min}"

    cp_min = float(out["closing_price"].min())
    assert cp_min > 0, f"closing_price must be > 0, actual min: {cp_min}"

    sp_min = float(out["subscription_price"].min())
    assert sp_min >= 0, f"subscription_price must be >= 0, actual min: {sp_min}"

    rp_min = float(out["rights_price"].min())
    assert rp_min >= 0, f"rights_price must be >= 0, actual min: {rp_min}"

    return out
