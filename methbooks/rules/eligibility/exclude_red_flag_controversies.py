"""
Purpose: Exclude companies flagged as Red Flag (MSCI Controversies Score of 0) for
    ongoing Very Severe controversies implicating the company directly.
Datapoints: msci_controversies_score.
Thresholds: 0 (Red Flag score).
Source: methbooks/data/markdown/MSCI_Climate_Action_Indexes_Methodology_20251211.md section "2.2.1 Controversies Score Eligibility" near line 140.
See also: methbooks/rules/event_handling/quarterly_controversies_bisr_deletion.py (re-applied at quarterly review).
"""
from __future__ import annotations

import polars as pl


def exclude_red_flag_controversies(df: pl.DataFrame) -> pl.DataFrame:
    out = df.filter(pl.col("msci_controversies_score") > 0)
    assert "msci_controversies_score" in out.columns, (
        f"msci_controversies_score column missing: {out.columns}"
    )
    assert out.height == 0 or out["msci_controversies_score"].min() > 0, (
        f"rows with msci_controversies_score == 0 (Red Flag) survived: min={out['msci_controversies_score'].min()}"
    )
    return out
