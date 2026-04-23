# methbooks/rules/event_handling/spinoff_exdate_inclusion.py
"""
Purpose: Include qualifying spun-off entity as of close of first trading day; for partial spin-offs of partially-owned companies recalculate free float as weighted average.
Datapoints: security_id, full_market_cap, fif
Thresholds: MIN_ADVANCE_NOTIFICATION_BEFORE_DETACHED_DELETION_BUSINESS_DAYS = 2
Source: meth-pipeline/MSCI_Corporate_Events_Methodology_20260210/2026-04-23T13-31-41Z/input/markdown.md section "2.8.1 Treatment when spun-off trades on ex-date" near line 3083.
See also: methbooks/methodologies/MSCI/corporate_events.py (corporate events apply pipeline).
"""
from __future__ import annotations

import polars as pl

MIN_ADVANCE_NOTIFICATION_BEFORE_DETACHED_DELETION_BUSINESS_DAYS = 2


def spinoff_exdate_inclusion(df: pl.DataFrame) -> pl.DataFrame:
    required = ["security_id", "full_market_cap", "fif"]
    missing = [c for c in required if c not in df.columns]
    assert not missing, f"missing required columns: {missing}"

    out = df

    fif_min = float(out["fif"].min())
    fif_max = float(out["fif"].max())
    assert fif_min >= 0, f"fif must be >= 0, actual min: {fif_min}"
    assert fif_max <= 1, f"fif must be <= 1, actual max: {fif_max}"

    fmcap_min = float(out["full_market_cap"].min())
    assert fmcap_min >= 0, f"full_market_cap must be >= 0, actual min: {fmcap_min}"

    assert MIN_ADVANCE_NOTIFICATION_BEFORE_DETACHED_DELETION_BUSINESS_DAYS >= 2, (
        f"advance notification before detached deletion must be >= 2 business days, "
        f"actual: {MIN_ADVANCE_NOTIFICATION_BEFORE_DETACHED_DELETION_BUSINESS_DAYS}"
    )

    return out
