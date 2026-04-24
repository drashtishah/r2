"""
Purpose: Preserve existing GICS classification during review by disregarding temporary fluctuations in results of a company's different activities.
Datapoints: primary_activity_revenue_pct, current_gics_sub_industry_code, revenue_trend_flag.
Thresholds: none.
Source: methbooks/data/markdown/MSCI_Global_Industry_Classification_Standard_(GICS®)_Methodology_20250220.md section "Section 4: Review of GICS Classification" near line 843.
See also: methbooks/rules/maintenance/restrict_reclassification_to_sustained_60pct.py.
"""
from __future__ import annotations

import polars as pl


def minimize_reclassification_on_temporary_fluctuations(df: pl.DataFrame) -> pl.DataFrame:
    out = df.with_columns(
        pl.when(pl.col("revenue_trend_flag"))
        .then(pl.col("current_gics_sub_industry_code"))
        .otherwise(pl.col("gics_sub_industry_code"))
        .alias("gics_sub_industry_code")
    )
    assert "revenue_trend_flag" in out.columns, (
        f"revenue_trend_flag column missing: {out.columns}"
    )
    assert out["revenue_trend_flag"].dtype == pl.Boolean, (
        f"revenue_trend_flag must be Boolean, got {out['revenue_trend_flag'].dtype}"
    )
    trend_rows = out.filter(pl.col("revenue_trend_flag"))
    assert (trend_rows["gics_sub_industry_code"] == trend_rows["current_gics_sub_industry_code"]).all(), (
        "rows with revenue_trend_flag == True do not have current_gics_sub_industry_code restored"
    )
    return out
