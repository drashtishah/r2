"""
Purpose: Maintain existing index constituents in the Eligible Universe at Annual Index Review if their Controversies Score is 1 or above, providing a stability buffer relative to the stricter threshold for new additions.
Datapoints: msci_controversies_score, is_current_constituent.
Thresholds: MIN_CONTROVERSIES_SCORE_EXISTING = 1 (module constant).
Source: methbooks/data/markdown/MSCI_Selection_Indexes_Methodology_20251211.md section "3.1.1 Updating the Eligible Universe" near line 353.
See also: methbooks/rules/eligibility/require_controversies_score_3_or_above.py (stricter threshold for new candidates, applied before this rule).
"""
from __future__ import annotations
import polars as pl

MIN_CONTROVERSIES_SCORE_EXISTING = 1

def retain_existing_if_controversies_score_1_or_above(df: pl.DataFrame) -> pl.DataFrame:
    out = df.filter(
        (~pl.col("is_current_constituent"))
        | (pl.col("msci_controversies_score") >= MIN_CONTROVERSIES_SCORE_EXISTING)
    )
    assert "msci_controversies_score" in out.columns, f"msci_controversies_score column missing: {out.columns}"
    assert "is_current_constituent" in out.columns, f"is_current_constituent column missing: {out.columns}"
    existing_low = out.filter(
        pl.col("is_current_constituent") & (pl.col("msci_controversies_score") < MIN_CONTROVERSIES_SCORE_EXISTING)
    )
    assert existing_low.height == 0, (
        f"existing constituent rows with msci_controversies_score < {MIN_CONTROVERSIES_SCORE_EXISTING} survived: "
        f"{existing_low.height} rows, scores={existing_low['msci_controversies_score'].to_list()}"
    )
    return out
