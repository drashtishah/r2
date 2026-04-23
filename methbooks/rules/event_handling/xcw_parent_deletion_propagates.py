"""
Purpose: Delete a constituent from this index at event time when it is deleted from
    the parent GIMI index; parent deletions are reflected simultaneously.
Datapoints: is_parent_deletion_event.
Thresholds: none.
Source: meth-pipeline/MSCI_Global_ex_Controversial_Weapons_Indexes_Methodology_20251211/2026-04-23T20-59-13Z/input/markdown.md section "3.2 Ongoing Event-Related Maintenance" near line 154.
See also: methbooks/rules/event_handling/xcw_characteristics_change_no_deletion.py (contrast: characteristics changes are not propagated; parent deletions are).
"""
from __future__ import annotations

import polars as pl


def xcw_parent_deletion_propagates(df: pl.DataFrame) -> pl.DataFrame:
    out = df.filter(pl.col("is_parent_deletion_event") == False)  # noqa: E712
    assert "is_parent_deletion_event" in out.columns, (
        f"is_parent_deletion_event column missing: {out.columns}"
    )
    assert out["is_parent_deletion_event"].sum() == 0, (
        f"parent-deleted securities survived filter: {out['is_parent_deletion_event'].sum()} rows"
    )
    return out
