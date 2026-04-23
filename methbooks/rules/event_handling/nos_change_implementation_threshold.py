# methbooks/rules/event_handling/nos_change_implementation_threshold.py
"""
Purpose: Implement NOS/FIF/DIF changes from primary equity offerings, secondary offerings/block sales, and debt-equity swaps at the time of the event only when the NOS change meets or exceeds the size-segment threshold; defer changes below threshold to next Index Review.
Datapoints: security_id, nos, size_segment
Thresholds: NOS_THRESHOLD_STANDARD_PCT = 5, NOS_THRESHOLD_SMALL_CAP_PCT = 10, NOS_THRESHOLD_MICRO_CAP_PCT = 25
Source: meth-pipeline/MSCI_Corporate_Events_Methodology_20260210/2026-04-23T13-31-41Z/input/markdown.md section "4.2 Implementation Thresholds" near line 4726.
See also: methbooks/methodologies/MSCI/corporate_events.py (corporate events apply pipeline).
"""
from __future__ import annotations

import polars as pl

NOS_THRESHOLD_STANDARD_PCT = 5
NOS_THRESHOLD_SMALL_CAP_PCT = 10
NOS_THRESHOLD_MICRO_CAP_PCT = 25


def nos_change_implementation_threshold(df: pl.DataFrame) -> pl.DataFrame:
    required = ["security_id", "nos", "size_segment"]
    missing = [c for c in required if c not in df.columns]
    assert not missing, f"missing required columns: {missing}"

    out = df

    nos_min = float(out["nos"].min())
    assert nos_min >= 0, f"nos must be >= 0, actual min: {nos_min}"

    valid_size_segments = {"Standard", "Small Cap", "Micro Cap"}
    actual_segments = set(out["size_segment"].unique().to_list())
    unknown = actual_segments - valid_size_segments
    assert not unknown, (
        f"unrecognized size_segment values: {unknown}; "
        f"expected one of {valid_size_segments}"
    )

    return out
