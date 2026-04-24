"""
Purpose: Announce Index Review results at least two calendar weeks before the
effective date (close of last business day of February, May, August, or November).
Datapoints: review_effective_date, announcement_date.
Thresholds: MIN_ANNOUNCEMENT_LEAD_WEEKS=2.
Source: meth-pipeline/MSCI_Global_Investable_Market_Indexes_Methodology_20260331/2026-04-24T07-46-35Z/input/markdown.md section "3.3 Announcement Policy" near line 3256.
See also: methbooks/rules/maintenance/gics_non_event_change_announcement_schedule.py (GICS-specific announcement schedule).
"""
from __future__ import annotations

import polars as pl

MIN_ANNOUNCEMENT_LEAD_WEEKS = 2
MIN_ANNOUNCEMENT_LEAD_DAYS = MIN_ANNOUNCEMENT_LEAD_WEEKS * 7


def announce_index_review_results(df: pl.DataFrame) -> pl.DataFrame:
    assert "review_effective_date" in df.columns, (
        f"review_effective_date column missing: {df.columns}"
    )
    assert "announcement_date" in df.columns, (
        f"announcement_date column missing: {df.columns}"
    )
    lead_days = (
        pl.col("review_effective_date").cast(pl.Date)
        - pl.col("announcement_date").cast(pl.Date)
    ).dt.total_days()
    bad = df.filter(lead_days < MIN_ANNOUNCEMENT_LEAD_DAYS)
    assert bad.height == 0, (
        f"announcement_date less than {MIN_ANNOUNCEMENT_LEAD_DAYS} calendar days"
        f" before review_effective_date: {bad.height} rows;"
        f" min lead days={df.select(lead_days.alias('lead')).get_column('lead').min()}"
    )
    return df
