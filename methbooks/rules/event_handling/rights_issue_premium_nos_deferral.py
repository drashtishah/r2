# methbooks/rules/event_handling/rights_issue_premium_nos_deferral.py
"""
Purpose: Defer NOS and FIF/DIF changes for premium rights issues until results are announced and the change meets the 5/10/25% threshold.
Datapoints: security_id, nos, fif, dif, subscription_price, closing_price, size_segment
Thresholds: NOS_THRESHOLD_STANDARD_PCT = 5, NOS_THRESHOLD_SMALL_CAP_PCT = 10, NOS_THRESHOLD_MICRO_CAP_PCT = 25
Source: meth-pipeline/MSCI_Corporate_Events_Methodology_20260210/2026-04-23T13-31-41Z/input/markdown.md section "3.6.3 Number of Shares, FIF and/or DIF Changes Following Rights" near line 4204.
See also: methbooks/methodologies/MSCI/corporate_events.py (corporate events apply pipeline).
"""
from __future__ import annotations

import polars as pl

NOS_THRESHOLD_STANDARD_PCT = 5
NOS_THRESHOLD_SMALL_CAP_PCT = 10
NOS_THRESHOLD_MICRO_CAP_PCT = 25


def rights_issue_premium_nos_deferral(df: pl.DataFrame) -> pl.DataFrame:
    required = ["security_id", "nos", "fif", "dif", "subscription_price", "closing_price", "size_segment"]
    missing = [c for c in required if c not in df.columns]
    assert not missing, f"missing required columns: {missing}"

    out = df

    nos_min = float(out["nos"].min())
    assert nos_min >= 0, f"nos must be >= 0, actual min: {nos_min}"

    # No NOS change must be implemented before results are announced for premium rights issues.
    if "results_announced" in out.columns and "nos_changed" in out.columns:
        premium_rows = out.filter(pl.col("subscription_price") >= pl.col("closing_price"))
        premature_change = premium_rows.filter(
            ~pl.col("results_announced") & pl.col("nos_changed")
        )
        assert premature_change.is_empty(), (
            f"no NOS change implemented before results announced for premium rights issue; "
            f"offending security_ids: {premature_change['security_id'].to_list()}"
        )

    return out
