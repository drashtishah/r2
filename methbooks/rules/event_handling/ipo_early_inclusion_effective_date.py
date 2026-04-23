# methbooks/rules/event_handling/ipo_early_inclusion_effective_date.py
"""
Purpose: Include qualifying IPOs in the Standard Index as of the close of the tenth trading day.
Datapoints: security_id
Thresholds: INCLUSION_TRADING_DAY = 10
Source: meth-pipeline/MSCI_Corporate_Events_Methodology_20260210/2026-04-23T13-31-41Z/input/markdown.md section "6 IPOs and Other Early Inclusions" near line 5229.
See also: methbooks/methodologies/MSCI/corporate_events.py (corporate events apply pipeline).
"""
from __future__ import annotations

import polars as pl

INCLUSION_TRADING_DAY = 10


def ipo_early_inclusion_effective_date(df: pl.DataFrame) -> pl.DataFrame:
    required = ["security_id"]
    missing = [c for c in required if c not in df.columns]
    assert not missing, f"missing required columns: {missing}"

    out = df

    assert INCLUSION_TRADING_DAY == 10, (
        f"IPO inclusion must be as of close of the tenth trading day, "
        f"actual INCLUSION_TRADING_DAY: {INCLUSION_TRADING_DAY}"
    )

    # IPO scheduled 5 days before or 3 days after Index Review effective date deferred to
    # coincide with Index Review.
    if "days_to_index_review" in out.columns and "deferred_to_index_review" in out.columns:
        near_review = out.filter(
            (pl.col("days_to_index_review") >= -3) & (pl.col("days_to_index_review") <= 5)
        )
        not_deferred = near_review.filter(~pl.col("deferred_to_index_review").cast(pl.Boolean))
        assert not_deferred.is_empty(), (
            f"IPO scheduled within Index Review window must be deferred to coincide with Index Review; "
            f"offending security_ids: {not_deferred['security_id'].to_list()}"
        )

    return out
