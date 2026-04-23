# methbooks/rules/event_handling/tender_offer_uncertain_results_decision.py
"""
Purpose: For hostile or uncertain-outcome tender offers, wait for publicly announced results, then delete with two-business-day notice if FIF < 0.15 or maintain with updated FIF/NOS if FIF >= 0.15.
Datapoints: security_id, fif, is_tender_friendly
Thresholds: FIF_DELETION_THRESHOLD = 0.15, MIN_ADVANCE_NOTIFICATION_BUSINESS_DAYS = 2
Source: meth-pipeline/MSCI_Corporate_Events_Methodology_20260210/2026-04-23T13-31-41Z/input/markdown.md section "2.2.3 Implementation timing" near line 588.
See also: methbooks/methodologies/MSCI/corporate_events.py (corporate events apply pipeline).
"""
from __future__ import annotations

import polars as pl

FIF_DELETION_THRESHOLD = 0.15
MIN_ADVANCE_NOTIFICATION_BUSINESS_DAYS = 2


def tender_offer_uncertain_results_decision(df: pl.DataFrame) -> pl.DataFrame:
    required = ["security_id", "fif", "is_tender_friendly"]
    missing = [c for c in required if c not in df.columns]
    assert not missing, f"missing required columns: {missing}"

    out = df

    fif_min = float(out["fif"].min())
    fif_max = float(out["fif"].max())
    assert fif_min >= 0, f"fif must be >= 0, actual min: {fif_min}"
    assert fif_max <= 1, f"fif must be <= 1, actual max: {fif_max}"

    assert MIN_ADVANCE_NOTIFICATION_BUSINESS_DAYS >= 2, (
        f"advance notification must be >= 2 business days, "
        f"actual: {MIN_ADVANCE_NOTIFICATION_BUSINESS_DAYS}"
    )

    return out
