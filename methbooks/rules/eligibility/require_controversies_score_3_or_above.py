"""
Purpose: Exclude new index candidates involved in very serious controversies by requiring an MSCI Controversies Score of 3 or above; applies only to non-constituents.
Datapoints: msci_controversies_score, is_current_constituent.
Thresholds: MIN_CONTROVERSIES_SCORE_NEW = 3 (module constant).
Source: methbooks/data/markdown/MSCI_Selection_Indexes_Methodology_20251211.md section "2.2.2 Controversies Score Eligibility" near line 149.
See also: methbooks/rules/eligibility/retain_existing_if_controversies_score_1_or_above.py (relaxed threshold for existing constituents).
"""
from __future__ import annotations
import polars as pl

MIN_CONTROVERSIES_SCORE_NEW = 3

def require_controversies_score_3_or_above(df: pl.DataFrame) -> pl.DataFrame:
    out = df.filter(
        pl.col("is_current_constituent")
        | (pl.col("msci_controversies_score") >= MIN_CONTROVERSIES_SCORE_NEW)
    )
    assert "msci_controversies_score" in out.columns, f"msci_controversies_score column missing: {out.columns}"
    assert "is_current_constituent" in out.columns, f"is_current_constituent column missing: {out.columns}"
    non_constituent_low = out.filter(
        (~pl.col("is_current_constituent")) & (pl.col("msci_controversies_score") < MIN_CONTROVERSIES_SCORE_NEW)
    )
    assert non_constituent_low.height == 0, (
        f"non-constituent rows with msci_controversies_score < {MIN_CONTROVERSIES_SCORE_NEW} survived: "
        f"{non_constituent_low.height} rows, scores={non_constituent_low['msci_controversies_score'].to_list()}"
    )
    return out
