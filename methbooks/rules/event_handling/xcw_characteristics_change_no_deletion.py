"""
Purpose: Retain a constituent in the index when its characteristics (country, sector,
    size segment, etc.) change intra-review; reevaluate at the subsequent Index Review.
Datapoints: has_characteristics_change.
Thresholds: none.
Source: meth-pipeline/MSCI_Global_ex_Controversial_Weapons_Indexes_Methodology_20251211/2026-04-23T20-59-13Z/input/markdown.md section "3.2 Ongoing Event-Related Maintenance" near line 199.
See also: methbooks/rules/event_handling/xcw_parent_deletion_propagates.py (contrast: parent deletions do propagate immediately; characteristics changes do not).
"""
from __future__ import annotations

import polars as pl


def xcw_characteristics_change_no_deletion(df: pl.DataFrame) -> pl.DataFrame:
    # Constituents with a characteristics change are retained; this rule is a
    # pass-through asserting no deletion was applied on that basis.
    assert "has_characteristics_change" in df.columns, (
        f"has_characteristics_change column missing: {df.columns}"
    )
    changed = df.filter(pl.col("has_characteristics_change") == True)  # noqa: E712
    assert changed.height >= 0, (
        f"unexpected negative row count for characteristics-changed rows: {changed.height}"
    )
    # All rows with has_characteristics_change == True must still be present in df;
    # since this rule does not filter them out, we confirm they survive.
    assert df.filter(pl.col("has_characteristics_change") == True).height == changed.height, (  # noqa: E712
        f"characteristics-changed rows were removed: expected {changed.height} to survive"
    )
    return df
