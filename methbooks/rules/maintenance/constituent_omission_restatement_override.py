"""
Purpose: Restate history for errors in the list of index constituents (e.g., a security unintentionally omitted) even when aggregate performance impact is not deemed important.
Datapoints: error_type, is_constituent_omission, restatement_flag.
Thresholds: none.
Source: methbooks/data/markdown/MSCI_Index_Policies.md section "Corrections Policy" near line 488.
See also: methbooks/rules/maintenance/correction_restatement_threshold.py (impact-based restatement trigger).
"""
from __future__ import annotations
import polars as pl


def constituent_omission_restatement_override(df: pl.DataFrame) -> pl.DataFrame:
    required = ["error_type", "is_constituent_omission", "restatement_flag"]
    missing = [c for c in required if c not in df.columns]
    assert not missing, f"missing required columns: {missing}"

    out = df
    omissions = out.filter(pl.col("is_constituent_omission"))
    if omissions.height > 0:
        not_flagged = omissions.filter(~pl.col("restatement_flag"))
        assert not_flagged.height == 0, (
            f"restatement_flag must be True where is_constituent_omission is True "
            f"regardless of error_impact_bps; unflagged_count={not_flagged.height}"
        )
    return out
