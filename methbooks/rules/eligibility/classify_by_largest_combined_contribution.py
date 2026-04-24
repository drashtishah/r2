"""
Purpose: When no single activity reaches 50% of both revenues and earnings, assign the Sub-Industry representing the largest combined contribution to revenues and earnings.
Datapoints: primary_activity_revenue_pct, primary_activity_earnings_pct.
Thresholds: none.
Source: methbooks/data/markdown/MSCI_Global_Industry_Classification_Standard_(GICS®)_Methodology_20250220.md section "Section 3.1: Classification by revenue and earnings" near line 796.
See also: methbooks/rules/eligibility/classify_by_majority_revenue_and_earnings.py.
"""
from __future__ import annotations

import polars as pl


def classify_by_largest_combined_contribution(df: pl.DataFrame) -> pl.DataFrame:
    out = df
    assert "primary_activity_revenue_pct" in out.columns, (
        f"primary_activity_revenue_pct column missing: {out.columns}"
    )
    assert "primary_activity_earnings_pct" in out.columns, (
        f"primary_activity_earnings_pct column missing: {out.columns}"
    )
    assert out["gics_sub_industry_code"].null_count() == 0, (
        f"null gics_sub_industry_code found after largest combined contribution step: "
        f"null_count={out['gics_sub_industry_code'].null_count()}"
    )
    return out
