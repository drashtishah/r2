# methbooks/rules/event_handling/ma_advance_notification_minimum.py
"""
Purpose: Provide at least two full business days advance notification for M&A deletions; MSCI may delay implementation beyond the last trading day to meet this minimum unless that would create replicability issues.
Datapoints: security_id
Thresholds: MIN_ADVANCE_NOTIFICATION_BUSINESS_DAYS = 2
Source: meth-pipeline/MSCI_Corporate_Events_Methodology_20260210/2026-04-23T13-31-41Z/input/markdown.md section "2 M&A preamble and 2.1.1" near line 358.
See also: methbooks/methodologies/MSCI/corporate_events.py (corporate events apply pipeline).
"""
from __future__ import annotations

import polars as pl

MIN_ADVANCE_NOTIFICATION_BUSINESS_DAYS = 2


def ma_advance_notification_minimum(df: pl.DataFrame) -> pl.DataFrame:
    required = ["security_id"]
    missing = [c for c in required if c not in df.columns]
    assert not missing, f"missing required columns: {missing}"

    out = df

    assert MIN_ADVANCE_NOTIFICATION_BUSINESS_DAYS >= 2, (
        f"advance notification must be >= 2 business days, "
        f"actual: {MIN_ADVANCE_NOTIFICATION_BUSINESS_DAYS}"
    )

    if "advance_notification_business_days" in out.columns:
        violations = out.filter(
            pl.col("advance_notification_business_days") < MIN_ADVANCE_NOTIFICATION_BUSINESS_DAYS
        )
        assert violations.is_empty(), (
            f"advance notification < {MIN_ADVANCE_NOTIFICATION_BUSINESS_DAYS} business days "
            f"for security_ids: {violations['security_id'].to_list()}"
        )

    return out
