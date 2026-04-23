"""
Purpose: Do not add spin-off securities created from an existing index constituent
    to this index at event implementation; reevaluate at the subsequent Index Review.
Datapoints: is_spinoff_from_constituent.
Thresholds: none.
Source: meth-pipeline/MSCI_Global_ex_Controversial_Weapons_Indexes_Methodology_20251211/2026-04-23T20-59-13Z/input/markdown.md section "3.2 Ongoing Event-Related Maintenance" near line 175.
See also: methbooks/rules/event_handling/xcw_new_addition_deferred_to_review.py (analogous deferral for parent index additions).
"""
from __future__ import annotations

import polars as pl


def xcw_spinoff_deferred_to_review(df: pl.DataFrame) -> pl.DataFrame:
    out = df.filter(pl.col("is_spinoff_from_constituent") == False)  # noqa: E712
    assert "is_spinoff_from_constituent" in out.columns, (
        f"is_spinoff_from_constituent column missing: {out.columns}"
    )
    assert out["is_spinoff_from_constituent"].sum() == 0, (
        f"spin-off securities survived filter: {out['is_spinoff_from_constituent'].sum()} rows"
    )
    return out
