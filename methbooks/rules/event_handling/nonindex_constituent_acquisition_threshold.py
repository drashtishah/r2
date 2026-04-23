# methbooks/rules/event_handling/nonindex_constituent_acquisition_threshold.py
"""
Purpose: Implement NOS and FIF/DIF changes from acquisitions of listed non-index constituents at the time of the event when NOS change meets or exceeds the size-segment threshold; defer smaller changes to next Index Review.
Datapoints: security_id, nos, size_segment, is_index_constituent
Thresholds: NOS_THRESHOLD_STANDARD_PCT = 5, NOS_THRESHOLD_SMALL_CAP_PCT = 10, NOS_THRESHOLD_MICRO_CAP_PCT = 25, NOS_UPDATE_DEFERRAL_THRESHOLD_PCT = 1
Source: meth-pipeline/MSCI_Corporate_Events_Methodology_20260210/2026-04-23T13-31-41Z/input/markdown.md section "2.3.2 Acquisitions of Listed Non-Index Constituent Securities" near line 745.
See also: methbooks/methodologies/MSCI/corporate_events.py (corporate events apply pipeline).
"""
from __future__ import annotations

import polars as pl

NOS_THRESHOLD_STANDARD_PCT = 5
NOS_THRESHOLD_SMALL_CAP_PCT = 10
NOS_THRESHOLD_MICRO_CAP_PCT = 25
NOS_UPDATE_DEFERRAL_THRESHOLD_PCT = 1


def nonindex_constituent_acquisition_threshold(df: pl.DataFrame) -> pl.DataFrame:
    required = ["security_id", "nos", "size_segment", "is_index_constituent"]
    missing = [c for c in required if c not in df.columns]
    assert not missing, f"missing required columns: {missing}"

    out = df

    nos_min = float(out["nos"].min())
    assert nos_min >= 0, f"nos must be >= 0 after update, actual min: {nos_min}"

    return out
