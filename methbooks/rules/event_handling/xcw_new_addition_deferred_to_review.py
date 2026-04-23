"""
Purpose: Do not add securities newly added to the parent GIMI index (IPOs, other
    early inclusions, size-segment migrations) to this index at event implementation;
    consider them at the subsequent Index Review.
Datapoints: is_new_parent_addition.
Thresholds: none.
Source: meth-pipeline/MSCI_Global_ex_Controversial_Weapons_Indexes_Methodology_20251211/2026-04-23T20-59-13Z/input/markdown.md section "3.2 Ongoing Event-Related Maintenance" near line 164.
See also: methbooks/rules/event_handling/xcw_spinoff_deferred_to_review.py (analogous deferral for spin-offs).
"""
from __future__ import annotations

import polars as pl


def xcw_new_addition_deferred_to_review(df: pl.DataFrame) -> pl.DataFrame:
    out = df.filter(pl.col("is_new_parent_addition") == False)  # noqa: E712
    assert "is_new_parent_addition" in out.columns, (
        f"is_new_parent_addition column missing: {out.columns}"
    )
    assert out["is_new_parent_addition"].sum() == 0, (
        f"new parent additions survived filter: {out['is_new_parent_addition'].sum()} rows"
    )
    return out
