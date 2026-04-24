"""
Purpose: Reflect Parent Index deletions in the Index simultaneously with the Parent Index so that securities removed from the Parent are also removed from the Index at the same time, maintaining alignment between the two.
Datapoints: parent_index_weight.
Thresholds: none.
Source: methbooks/data/markdown/MSCI_EU_CTB_PAB_Overlay_Indexes_Methodology_20260211.md section "3.2 Ongoing Event Related Changes" near line 553.
See also: methbooks/rules/event_handling/neutralize_intermediate_parent_reviews.py (companion rule for intermediate-review neutralization).
"""
from __future__ import annotations
import polars as pl


def reflect_parent_deletions(df: pl.DataFrame) -> pl.DataFrame:
    """Proxy: in a live pipeline, removes rows where parent_index_weight is null
    or 0.0 (securities deleted from the Parent Index at event time). In the mock
    pipeline all parent_index_weight values are positive, so no rows are dropped.
    """
    out = df.filter(
        pl.col("parent_index_weight").is_not_null()
        & (pl.col("parent_index_weight") > 0)
    )
    assert "weight" in out.columns, f"weight column missing after parent-deletion reflection: {out.columns}"
    assert out.height > 0, f"No constituents after reflecting parent deletions: height={out.height}"
    return out
