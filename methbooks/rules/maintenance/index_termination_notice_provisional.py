"""
Purpose: Ensure provisional indexes created to reflect future methodology changes receive at least 1 week of advance termination notice.
Datapoints: is_provisional_index, termination_announcement_date, termination_effective_date.
Thresholds: MINIMUM_NOTICE_DAYS = 7.
Source: methbooks/data/markdown/MSCI_Index_Policies.md section "Index Termination Policy" near line 460.
See also: methbooks/rules/maintenance/index_termination_notice_standard.py (standard index variant).
"""
from __future__ import annotations
import polars as pl

MINIMUM_NOTICE_DAYS = 7


def index_termination_notice_provisional(df: pl.DataFrame) -> pl.DataFrame:
    required = ["is_provisional_index", "termination_announcement_date", "termination_effective_date"]
    missing = [c for c in required if c not in df.columns]
    assert not missing, f"missing required columns: {missing}"

    out = df
    provisional = out.filter(pl.col("is_provisional_index"))
    if provisional.height > 0:
        notice_days = (
            pl.col("termination_effective_date") - pl.col("termination_announcement_date")
        ).dt.total_days()
        short_notice = provisional.filter(notice_days < MINIMUM_NOTICE_DAYS)
        assert short_notice.height == 0, (
            f"provisional index terminations must have at least {MINIMUM_NOTICE_DAYS} days notice; "
            f"short_notice_count={short_notice.height}"
        )
    return out
