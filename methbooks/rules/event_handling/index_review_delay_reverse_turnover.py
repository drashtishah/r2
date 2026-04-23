# methbooks/rules/event_handling/index_review_delay_reverse_turnover.py
"""
Purpose: For securities scheduled for add/delete/migration at an upcoming Index Review, delay intra-event NOS/FIF/DIF changes to coincide with the Index Review effective date to reduce reverse turnover risk.
Datapoints: security_id, size_segment, is_pending_index_review_action
Thresholds: none
Source: meth-pipeline/MSCI_Corporate_Events_Methodology_20260210/2026-04-23T13-31-41Z/input/markdown.md section "4.4 Index Review Treatment" near line 4883.
See also: methbooks/methodologies/MSCI/corporate_events.py (corporate events apply pipeline).
"""
from __future__ import annotations

import polars as pl


def index_review_delay_reverse_turnover(df: pl.DataFrame) -> pl.DataFrame:
    required = ["security_id", "size_segment", "is_pending_index_review_action"]
    missing = [c for c in required if c not in df.columns]
    assert not missing, f"missing required columns: {missing}"

    out = df

    # For securities migrating between Standard and Small Cap, threshold for delay uses
    # the larger size segment.
    if "migration_target_segment" in out.columns and "threshold_size_segment_used" in out.columns:
        migration_rows = out.filter(
            pl.col("is_pending_index_review_action")
            & pl.col("migration_target_segment").is_not_null()
        )
        for row in migration_rows.to_dicts():
            segments = {row["size_segment"], row["migration_target_segment"]}
            larger = "Standard" if "Standard" in segments else row["size_segment"]
            assert row["threshold_size_segment_used"] == larger, (
                f"threshold for delay assessment must use larger size segment when migrating; "
                f"security_id: {row['security_id']}, expected: {larger}, "
                f"actual: {row['threshold_size_segment_used']}"
            )

    return out
