"""
Purpose: Use pro forma post-event fundamental data for style review when a significant M&A, spin-off inclusion, or IPO changes full market cap or shares outstanding by 50% or more increase or 33% or more decrease, provided the post-event entity moves between specified size segments.
Datapoints: market_cap_pre_event, market_cap_post_event, shares_outstanding_pre_event, shares_outstanding_post_event, index_size_segment_pre_event, index_size_segment_post_event.
Thresholds: MCAP_INCREASE_THRESHOLD = 0.5, MCAP_DECREASE_THRESHOLD = 0.33, SHARES_INCREASE_THRESHOLD = 0.5, SHARES_DECREASE_THRESHOLD = 0.33.
Source: methbooks/data/markdown/MSCI_Fundamental_Data_Methodology_20240625.md section "4.4 Changes in Business Structure" near line 3669.
See also: methbooks/rules/event_handling/apply_proforma_for_loss_making_acquisition.py (related pro forma trigger for loss-making acquisitions).
"""
from __future__ import annotations

import polars as pl

MCAP_INCREASE_THRESHOLD = 0.5
MCAP_DECREASE_THRESHOLD = 0.33
SHARES_INCREASE_THRESHOLD = 0.5
SHARES_DECREASE_THRESHOLD = 0.33

_SEGMENT_TRANSITIONS = {("non_constituent", "gimi_constituent"), ("small_cap", "standard")}


def apply_proforma_fundamental_business_structure_change(df: pl.DataFrame) -> pl.DataFrame:
    for col in ("market_cap_pre_event", "market_cap_post_event",
                "shares_outstanding_pre_event", "shares_outstanding_post_event",
                "index_size_segment_pre_event", "index_size_segment_post_event"):
        assert col in df.columns, f"{col} column missing: {df.columns}"

    mcap_pre = float(df["market_cap_pre_event"].min())
    assert mcap_pre > 0, f"market_cap_pre_event must be positive: min={mcap_pre}"

    mcap_change = (
        (pl.col("market_cap_post_event") - pl.col("market_cap_pre_event"))
        / pl.col("market_cap_pre_event")
    )
    shares_change = (
        (pl.col("shares_outstanding_post_event") - pl.col("shares_outstanding_pre_event"))
        / pl.col("shares_outstanding_pre_event")
    )
    threshold_breached = (
        (mcap_change >= MCAP_INCREASE_THRESHOLD) | (mcap_change <= -MCAP_DECREASE_THRESHOLD)
        | (shares_change >= SHARES_INCREASE_THRESHOLD) | (shares_change <= -SHARES_DECREASE_THRESHOLD)
    )
    segment_transition = pl.lit(False)
    for pre, post in _SEGMENT_TRANSITIONS:
        segment_transition = segment_transition | (
            (pl.col("index_size_segment_pre_event") == pre)
            & (pl.col("index_size_segment_post_event") == post)
        )
    out = df.with_columns(
        pl.when(threshold_breached & segment_transition)
        .then(pl.lit(True))
        .otherwise(pl.lit(False))
        .alias("proforma_style_review_flag")
    )
    assert "proforma_style_review_flag" in out.columns, (
        f"proforma_style_review_flag column missing after transform: {out.columns}"
    )
    flagged = out.filter(pl.col("proforma_style_review_flag"))
    if flagged.height > 0:
        for row in flagged.iter_rows(named=True):
            pre = row["market_cap_pre_event"]
            post = row["market_cap_post_event"]
            s_pre = row["shares_outstanding_pre_event"]
            s_post = row["shares_outstanding_post_event"]
            mcap_ok = (post - pre) / pre >= MCAP_INCREASE_THRESHOLD or (post - pre) / pre <= -MCAP_DECREASE_THRESHOLD
            shares_ok = (s_post - s_pre) / s_pre >= SHARES_INCREASE_THRESHOLD or (s_post - s_pre) / s_pre <= -SHARES_DECREASE_THRESHOLD
            assert mcap_ok or shares_ok, (
                f"proforma_style_review_flag set but neither mcap nor shares threshold met: "
                f"mcap_change={(post-pre)/pre:.3f}, shares_change={(s_post-s_pre)/s_pre:.3f}"
            )
            transition = (row["index_size_segment_pre_event"], row["index_size_segment_post_event"])
            assert transition in _SEGMENT_TRANSITIONS, (
                f"proforma_style_review_flag set but segment transition not permitted: "
                f"transition={transition}"
            )
    return out
