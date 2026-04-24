"""
Purpose: Classify a tracking stock based on the underlying business it tracks rather than the issuing parent company.
Datapoints: is_tracking_stock, underlying_business_gics_sub_industry_code.
Thresholds: none.
Source: methbooks/data/markdown/MSCI_Global_Industry_Classification_Standard_(GICS®)_Methodology_20250220.md section "Section 3.2: Propagation of GICS from company level to security level" near line 819.
See also: methbooks/rules/maintenance/propagate_gics_from_company_to_equity_securities.py.
"""
from __future__ import annotations

import polars as pl


def classify_tracking_stock_by_underlying(df: pl.DataFrame) -> pl.DataFrame:
    out = df.with_columns(
        pl.when(pl.col("is_tracking_stock"))
        .then(pl.col("underlying_business_gics_sub_industry_code"))
        .otherwise(pl.col("gics_sub_industry_code"))
        .alias("gics_sub_industry_code")
    )
    assert "is_tracking_stock" in out.columns, f"is_tracking_stock column missing: {out.columns}"
    tracking_rows = out.filter(pl.col("is_tracking_stock"))
    assert tracking_rows["underlying_business_gics_sub_industry_code"].null_count() == 0, (
        "tracking stock rows have null underlying_business_gics_sub_industry_code"
    )
    assert (
        tracking_rows["gics_sub_industry_code"] == tracking_rows["underlying_business_gics_sub_industry_code"]
    ).all(), "tracking stock gics_sub_industry_code does not match underlying_business_gics_sub_industry_code"
    return out
