"""
Purpose: Ensure fixed income GICS classifications are reviewed at minimum annually and also upon significant corporate restructuring or client request.
Datapoints: fi_last_review_date, has_significant_restructuring, client_review_requested.
Thresholds: REVIEW_FREQUENCY_MONTHS = 12.
Source: methbooks/data/markdown/MSCI_Global_Industry_Classification_Standard_(GICS®)_Methodology_20250220.md section "Section 8.3.1: Ongoing review" near line 3909.
See also: methbooks/rules/event_handling/trigger_review_on_restructuring_or_new_annual_report.py.
"""
from __future__ import annotations

from datetime import date

import polars as pl

REVIEW_FREQUENCY_MONTHS = 12
REFERENCE_DATE = date(2026, 4, 24)


def review_fixed_income_gics_annually(df: pl.DataFrame) -> pl.DataFrame:
    out = df.with_columns(
        (
            (pl.col("fi_last_review_date") < pl.lit(REFERENCE_DATE).dt.offset_by(f"-{REVIEW_FREQUENCY_MONTHS}mo"))
            | pl.col("has_significant_restructuring")
            | pl.col("client_review_requested")
        ).alias("fi_review_due")
    )
    assert "fi_last_review_date" in out.columns, f"fi_last_review_date column missing: {out.columns}"
    assert out["fi_last_review_date"].null_count() == 0, "null fi_last_review_date values found"
    overdue = out.filter(
        pl.col("fi_last_review_date") < pl.lit(REFERENCE_DATE).dt.offset_by(f"-{REVIEW_FREQUENCY_MONTHS}mo")
    )
    assert overdue.height == 0, (
        f"fi_last_review_date more than {REVIEW_FREQUENCY_MONTHS} months before reference date: "
        f"offending company_id={overdue['security_id'][0] if overdue.height > 0 else None}"
    )
    restructuring_rows = out.filter(pl.col("has_significant_restructuring") | pl.col("client_review_requested"))
    assert restructuring_rows["fi_review_due"].all(), (
        "rows with has_significant_restructuring or client_review_requested do not have fi_review_due == True"
    )
    return out
