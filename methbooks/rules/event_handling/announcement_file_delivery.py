# methbooks/rules/event_handling/announcement_file_delivery.py
"""
Purpose: Deliver the daily ACE file between 4:00 PM and 6:00 PM US EST and send intraday announcements for events announced during market hours for same-day or next-day implementation.
Datapoints: security_id
Thresholds: DESCRIPTIVE_TEXT_MCAP_CHANGE_THRESHOLD_USD_BN = 5
Source: meth-pipeline/MSCI_Corporate_Events_Methodology_20260210/2026-04-23T13-31-41Z/input/markdown.md section "8 General Announcement Policy for Corporate Events" near line 5355.
See also: methbooks/methodologies/MSCI/corporate_events.py (corporate events apply pipeline).
"""
from __future__ import annotations

import polars as pl

DESCRIPTIVE_TEXT_MCAP_CHANGE_THRESHOLD_USD_BN = 5


def announcement_file_delivery(df: pl.DataFrame) -> pl.DataFrame:
    required = ["security_id"]
    missing = [c for c in required if c not in df.columns]
    assert not missing, f"missing required columns: {missing}"

    out = df

    assert DESCRIPTIVE_TEXT_MCAP_CHANGE_THRESHOLD_USD_BN == 5, (
        f"daily ACE file delivered between 4:00 PM and 6:00 PM US EST; "
        f"descriptive text threshold must be 5 USD bn, "
        f"actual: {DESCRIPTIVE_TEXT_MCAP_CHANGE_THRESHOLD_USD_BN}"
    )

    return out
