# methbooks/rules/event_handling/rights_issue_size_review_trigger.py
"""
Purpose: Trigger a downward size review when a rights issue is expected to increase full market cap by 50% or more.
Datapoints: security_id, full_market_cap, size_segment
Thresholds: SIZE_REVIEW_TRIGGER_MARKET_CAP_INCREASE_PCT = 50
Source: meth-pipeline/MSCI_Corporate_Events_Methodology_20260210/2026-04-23T13-31-41Z/input/markdown.md section "3.6.3 Number of Shares, FIF and/or DIF Changes Following Rights" near line 4204.
See also: methbooks/methodologies/MSCI/corporate_events.py (corporate events apply pipeline).
"""
from __future__ import annotations

import polars as pl

SIZE_REVIEW_TRIGGER_MARKET_CAP_INCREASE_PCT = 50


def rights_issue_size_review_trigger(df: pl.DataFrame) -> pl.DataFrame:
    required = ["security_id", "full_market_cap", "size_segment"]
    missing = [c for c in required if c not in df.columns]
    assert not missing, f"missing required columns: {missing}"

    out = df

    assert SIZE_REVIEW_TRIGGER_MARKET_CAP_INCREASE_PCT == 50, (
        f"size review must be triggered when projected full_market_cap increase >= 50%, "
        f"actual threshold: {SIZE_REVIEW_TRIGGER_MARKET_CAP_INCREASE_PCT}"
    )

    fmcap_min = float(out["full_market_cap"].min())
    assert fmcap_min >= 0, f"full_market_cap must be >= 0, actual min: {fmcap_min}"

    if "size_review_triggered" in out.columns and "full_market_cap_increase_pct" in out.columns:
        large_increase = out.filter(
            pl.col("full_market_cap_increase_pct") >= SIZE_REVIEW_TRIGGER_MARKET_CAP_INCREASE_PCT
        )
        not_triggered = large_increase.filter(~pl.col("size_review_triggered").cast(pl.Boolean))
        assert not_triggered.is_empty(), (
            f"size review must be triggered when full_market_cap increase >= "
            f"{SIZE_REVIEW_TRIGGER_MARKET_CAP_INCREASE_PCT}%; "
            f"offending security_ids: {not_triggered['security_id'].to_list()}"
        )

    return out
