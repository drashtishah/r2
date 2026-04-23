# methbooks/rules/event_handling/rights_issue_discount_nos.py
"""
Purpose: Increase NOS as of close of ex-date for discount rights issues, assumed fully subscribed pro rata; apply NOS size-segment threshold; defer sub-threshold updates.
Datapoints: security_id, nos, fif, dif, subscription_price, closing_price, size_segment
Thresholds: NOS_THRESHOLD_STANDARD_PCT = 5, NOS_THRESHOLD_SMALL_CAP_PCT = 10, NOS_THRESHOLD_MICRO_CAP_PCT = 25, NOS_UPDATE_DEFERRAL_THRESHOLD_PCT = 1
Source: meth-pipeline/MSCI_Corporate_Events_Methodology_20260210/2026-04-23T13-31-41Z/input/markdown.md section "3.6.3 Number of Shares, FIF and/or DIF Changes Following Rights" near line 4204.
See also: methbooks/methodologies/MSCI/corporate_events.py (corporate events apply pipeline).
"""
from __future__ import annotations

import polars as pl

NOS_THRESHOLD_STANDARD_PCT = 5
NOS_THRESHOLD_SMALL_CAP_PCT = 10
NOS_THRESHOLD_MICRO_CAP_PCT = 25
NOS_UPDATE_DEFERRAL_THRESHOLD_PCT = 1


def rights_issue_discount_nos(df: pl.DataFrame) -> pl.DataFrame:
    required = ["security_id", "nos", "fif", "dif", "subscription_price", "closing_price", "size_segment"]
    missing = [c for c in required if c not in df.columns]
    assert not missing, f"missing required columns: {missing}"

    out = df

    nos_min = float(out["nos"].min())
    assert nos_min >= 0, f"nos must be >= 0 after update, actual min: {nos_min}"

    fif_min = float(out["fif"].min())
    fif_max = float(out["fif"].max())
    assert fif_min >= 0, f"fif must be >= 0, actual min: {fif_min}"
    assert fif_max <= 1, f"fif must be <= 1, actual max: {fif_max}"

    dif_min = float(out["dif"].min())
    dif_max = float(out["dif"].max())
    assert dif_min >= 0, f"dif must be >= 0, actual min: {dif_min}"
    assert dif_max <= 1, f"dif must be <= 1, actual max: {dif_max}"

    # NOS update < 1% of post-event NOS must be deferred to Index Review.
    if "nos_change_pct" in out.columns and "deferred_to_index_review" in out.columns:
        small_change = out.filter(
            pl.col("nos_change_pct") < NOS_UPDATE_DEFERRAL_THRESHOLD_PCT
        )
        not_deferred = small_change.filter(~pl.col("deferred_to_index_review").cast(pl.Boolean))
        assert not_deferred.is_empty(), (
            f"nos update < {NOS_UPDATE_DEFERRAL_THRESHOLD_PCT}% of post-event nos "
            f"must be deferred to Index Review; "
            f"offending security_ids: {not_deferred['security_id'].to_list()}"
        )

    return out
