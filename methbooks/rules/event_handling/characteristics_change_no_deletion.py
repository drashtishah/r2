"""
Purpose: Preserve existing constituent through classification changes between reviews;
    defer reevaluation to next review.
Datapoints: security_id, is_current_constituent, characteristics_changed_flag.
Thresholds: none.
Source: methbooks/data/markdown/msci_quality_indexes_methodology_20250520.md section
    "3.2 Ongoing Event Related Changes" near line 357.
See also: methbooks/rules/event_handling/xcw_characteristics_change_no_deletion.py.
"""
from __future__ import annotations

import polars as pl


def characteristics_change_no_deletion(df: pl.DataFrame) -> pl.DataFrame:
    assert "security_id" in df.columns, f"security_id missing: {df.columns}"
    assert "is_current_constituent" in df.columns, (
        f"is_current_constituent missing: {df.columns}"
    )
    assert "characteristics_changed_flag" in df.columns, (
        f"characteristics_changed_flag missing: {df.columns}"
    )

    # Non-constituents with a characteristics change are not added mid-review.
    out = df.filter(
        ~(
            (pl.col("is_current_constituent") == False)  # noqa: E712
            & (pl.col("characteristics_changed_flag") == True)  # noqa: E712
        )
    )

    # All existing constituents with characteristics change must survive.
    surviving_ids = set(out["security_id"].to_list())
    existing_changed = df.filter(
        (pl.col("is_current_constituent") == True)  # noqa: E712
        & (pl.col("characteristics_changed_flag") == True)  # noqa: E712
    )
    missing = [sid for sid in existing_changed["security_id"].to_list() if sid not in surviving_ids]
    assert len(missing) == 0, (
        f"existing constituents with characteristics change removed from output: {missing}"
    )

    return out
