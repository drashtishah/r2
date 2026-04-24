"""
Purpose: Trigger historical index restatement when a data error's performance impact at the country or regional index level is deemed important.
Datapoints: error_impact_bps, index_level_scope, restatement_flag.
Thresholds: IMPORTANT_THRESHOLD_BPS = 50.
Source: methbooks/data/markdown/MSCI_Index_Policies.md section "Corrections Policy" near line 485.
See also: methbooks/rules/maintenance/constituent_omission_restatement_override.py (override for constituent omissions).
"""
from __future__ import annotations
import polars as pl

IMPORTANT_THRESHOLD_BPS = 50


def correction_restatement_threshold(df: pl.DataFrame) -> pl.DataFrame:
    required = ["error_impact_bps", "index_level_scope", "restatement_flag"]
    missing = [c for c in required if c not in df.columns]
    assert not missing, f"missing required columns: {missing}"

    out = df
    eligible = out.filter(
        (pl.col("error_impact_bps") >= IMPORTANT_THRESHOLD_BPS)
        & (pl.col("index_level_scope").is_in(["country", "regional"]))
    )
    if eligible.height > 0:
        not_flagged = eligible.filter(~pl.col("restatement_flag"))
        assert not_flagged.height == 0, (
            f"restatement_flag must be True for all records where error_impact_bps >= {IMPORTANT_THRESHOLD_BPS} "
            f"at country or regional index level; unflagged_count={not_flagged.height}"
        )
    return out
