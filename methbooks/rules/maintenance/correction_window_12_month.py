"""
Purpose: Exclude errors older than 12 months from the standard correction process; only errors within the rolling 12-month window are eligible for correction.
Datapoints: error_occurrence_date, error_discovery_date, correction_eligible_flag.
Thresholds: CORRECTION_WINDOW_MONTHS = 12.
Source: methbooks/data/markdown/MSCI_Index_Policies.md section "Corrections Policy" near line 493.
See also: methbooks/rules/maintenance/correction_restatement_threshold.py (restatement threshold rule).
"""
from __future__ import annotations
import polars as pl

CORRECTION_WINDOW_MONTHS = 12


def correction_window_12_month(df: pl.DataFrame) -> pl.DataFrame:
    required = ["error_occurrence_date", "error_discovery_date", "correction_eligible_flag"]
    missing = [c for c in required if c not in df.columns]
    assert not missing, f"missing required columns: {missing}"

    out = df
    # Compute age in days; 12 months approximated as 365 days.
    window_days = CORRECTION_WINDOW_MONTHS * 365 // 12

    old_errors = out.filter(
        (pl.col("error_discovery_date") - pl.col("error_occurrence_date")).dt.total_days()
        > window_days
    )
    if old_errors.height > 0:
        wrongly_eligible = old_errors.filter(pl.col("correction_eligible_flag"))
        assert wrongly_eligible.height == 0, (
            f"no error with discovery-to-occurrence gap > {CORRECTION_WINDOW_MONTHS} months "
            f"may be flagged eligible under the standard correction process; "
            f"wrongly_eligible_count={wrongly_eligible.height}"
        )
    return out
