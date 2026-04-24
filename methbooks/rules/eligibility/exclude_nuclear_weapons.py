"""
Purpose: Exclude companies flagged for involvement in nuclear weapons.
Datapoints: nuclear_weapons_flag.
Thresholds: none.
Source: methbooks/data/markdown/MSCI_Climate_Action_Indexes_Methodology_20251211.md section "Eligibility Criteria" near line 1.
See also: methbooks/rules/event_handling/quarterly_controversies_bisr_deletion.py (re-applied at quarterly review).
"""
from __future__ import annotations

import polars as pl


def exclude_nuclear_weapons(df: pl.DataFrame) -> pl.DataFrame:
    out = df.filter(pl.col("nuclear_weapons_flag") == False)  # noqa: E712
    assert "nuclear_weapons_flag" in out.columns, (
        f"nuclear_weapons_flag column missing: {out.columns}"
    )
    assert out["nuclear_weapons_flag"].sum() == 0, (
        f"nuclear weapons rows survived: {out['nuclear_weapons_flag'].sum()}"
    )
    return out
