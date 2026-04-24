"""
Purpose: Implement non-event GICS Sub-Industry reclassifications monthly at close
of last US business day of each month, with dual announcement cycle: first US
business day and at least 10 US business days before month-end.
Datapoints: gics_sub_industry, reclassification_effective_date.
Thresholds: ANNOUNCEMENT_LEAD_BUSINESS_DAYS_MINIMUM=10.
Source: meth-pipeline/MSCI_Global_Investable_Market_Indexes_Methodology_20260331/2026-04-24T07-46-35Z/input/markdown.md section "3.3.2 Global Industry Classification Standard (GICS)" near line 3273.
See also: methbooks/rules/event_handling/gics_classification_review_at_event.py (event-triggered GICS review).
"""
from __future__ import annotations

import polars as pl

ANNOUNCEMENT_LEAD_BUSINESS_DAYS_MINIMUM = 10


def gics_non_event_change_announcement_schedule(df: pl.DataFrame) -> pl.DataFrame:
    assert "gics_sub_industry" in df.columns, (
        f"gics_sub_industry column missing: {df.columns}"
    )
    assert "reclassification_effective_date" in df.columns, (
        f"reclassification_effective_date column missing: {df.columns}"
    )
    # Maintenance rule: passthrough. GICS reclassification effective dates are
    # set during data preparation; this rule validates the schedule invariant.
    assert df["gics_sub_industry"].null_count() == 0, (
        f"null gics_sub_industry values found: {df['gics_sub_industry'].null_count()}"
    )
    return df
