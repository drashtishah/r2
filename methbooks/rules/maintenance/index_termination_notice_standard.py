"""
Purpose: Guarantee market participants at least 3 months to transition before an index is terminated under standard circumstances.
Datapoints: termination_announcement_date, termination_effective_date, termination_cause.
Thresholds: MINIMUM_NOTICE_DAYS = 90.
Source: methbooks/data/markdown/MSCI_Index_Policies.md section "Index Termination Policy" near line 437.
See also: methbooks/rules/maintenance/index_termination_notice_provisional.py (provisional index variant).
"""
from __future__ import annotations
import polars as pl

MINIMUM_NOTICE_DAYS = 90


def index_termination_notice_standard(df: pl.DataFrame) -> pl.DataFrame:
    required = ["termination_announcement_date", "termination_effective_date", "termination_cause"]
    missing = [c for c in required if c not in df.columns]
    assert not missing, f"missing required columns: {missing}"

    out = df
    # Only check non-EIC-exception rows; termination_cause != "eic_exception" are standard.
    standard = out.filter(pl.col("termination_cause") != "eic_exception")
    if standard.height > 0:
        notice_days = (
            pl.col("termination_effective_date") - pl.col("termination_announcement_date")
        ).dt.total_days()
        short_notice = standard.filter(notice_days < MINIMUM_NOTICE_DAYS)
        assert short_notice.height == 0, (
            f"standard terminations must have at least {MINIMUM_NOTICE_DAYS} days notice; "
            f"short_notice_count={short_notice.height}"
        )
    return out
