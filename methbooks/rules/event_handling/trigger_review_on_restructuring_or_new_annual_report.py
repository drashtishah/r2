"""
Purpose: Initiate a GICS Sub-Industry review when a significant corporate restructuring occurs, a new annual report is published, or a client request is received.
Datapoints: has_significant_restructuring, has_new_annual_report, client_review_requested.
Thresholds: none.
Source: methbooks/data/markdown/MSCI_Global_Industry_Classification_Standard_(GICS®)_Methodology_20250220.md section "Section 4: Review of GICS Classification" near line 838.
See also: methbooks/rules/maintenance/minimize_reclassification_on_temporary_fluctuations.py.
"""
from __future__ import annotations

import polars as pl


def trigger_review_on_restructuring_or_new_annual_report(df: pl.DataFrame) -> pl.DataFrame:
    out = df.with_columns(
        (
            pl.col("has_significant_restructuring")
            | pl.col("has_new_annual_report")
            | pl.col("client_review_requested")
        ).alias("review_triggered")
    )
    assert "has_significant_restructuring" in out.columns, (
        f"has_significant_restructuring column missing: {out.columns}"
    )
    assert out["has_significant_restructuring"].dtype == pl.Boolean, (
        f"has_significant_restructuring must be Boolean, got {out['has_significant_restructuring'].dtype}"
    )
    assert "has_new_annual_report" in out.columns, (
        f"has_new_annual_report column missing: {out.columns}"
    )
    assert out["has_new_annual_report"].dtype == pl.Boolean, (
        f"has_new_annual_report must be Boolean, got {out['has_new_annual_report'].dtype}"
    )
    assert "client_review_requested" in out.columns, (
        f"client_review_requested column missing: {out.columns}"
    )
    assert out["client_review_requested"].dtype == pl.Boolean, (
        f"client_review_requested must be Boolean, got {out['client_review_requested'].dtype}"
    )
    trigger_rows = out.filter(
        pl.col("has_significant_restructuring")
        | pl.col("has_new_annual_report")
        | pl.col("client_review_requested")
    )
    assert trigger_rows["review_triggered"].all(), (
        "rows with at least one trigger condition do not have review_triggered == True"
    )
    return out
