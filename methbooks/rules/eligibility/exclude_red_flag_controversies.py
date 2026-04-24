"""
Purpose: Exclude companies with an MSCI controversies score above zero (red flag).
Datapoints: msci_controversies_score.
Thresholds: 0 (strict).
Source: methbooks/data/markdown/MSCI_Climate_Action_Indexes_Methodology_20251211.md section "Eligibility Criteria" near line 1.
See also: methbooks/rules/event_handling/quarterly_controversies_bisr_deletion.py (re-applied at quarterly review).
"""
from __future__ import annotations

import polars as pl


def exclude_red_flag_controversies(df: pl.DataFrame) -> pl.DataFrame:
    out = df.filter(pl.col("msci_controversies_score") == 0)
    assert "msci_controversies_score" in out.columns, (
        f"msci_controversies_score column missing: {out.columns}"
    )
    assert out["msci_controversies_score"].max() == 0 or out.height == 0, (
        f"rows with msci_controversies_score > 0 survived: {out['msci_controversies_score'].max()}"
    )
    return out
