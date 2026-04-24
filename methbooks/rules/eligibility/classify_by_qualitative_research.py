"""
Purpose: When none of the quantitative revenue or earnings thresholds are met, determine GICS classification through additional research and analysis.
Datapoints: primary_activity_revenue_pct, primary_activity_earnings_pct, qualitative_classification_flag.
Thresholds: none.
Source: methbooks/data/markdown/MSCI_Global_Industry_Classification_Standard_(GICS®)_Methodology_20250220.md section "Section 3.1: Classification by revenue and earnings" near line 800.
See also: methbooks/rules/eligibility/classify_by_largest_combined_contribution.py.
"""
from __future__ import annotations

import polars as pl


def classify_by_qualitative_research(df: pl.DataFrame) -> pl.DataFrame:
    out = df
    assert "qualitative_classification_flag" in out.columns, (
        f"qualitative_classification_flag column missing: {out.columns}"
    )
    assert out["qualitative_classification_flag"].dtype == pl.Boolean, (
        f"qualitative_classification_flag must be Boolean, got {out['qualitative_classification_flag'].dtype}"
    )
    qual_rows = out.filter(pl.col("qualitative_classification_flag"))
    assert qual_rows["gics_sub_industry_code"].null_count() == 0, (
        f"qualitatively classified rows have null gics_sub_industry_code: count={qual_rows.height}"
    )
    return out
