"""
Purpose: When no parent GICS is available in GICS Direct, classify the bond-only issuer based on its own or related entities' underlying business using the GICS framework.
Datapoints: has_issued_equity, parent_gics_sub_industry_code, primary_activity_revenue_pct.
Thresholds: none.
Source: methbooks/data/markdown/MSCI_Global_Industry_Classification_Standard_(GICS®)_Methodology_20250220.md section "Section 8.1.2: Classification of a company that has issued only corporate bonds" near line 3881.
See also: methbooks/rules/eligibility/classify_bond_only_issuer_by_parent_gics.py.
"""
from __future__ import annotations

import polars as pl


def classify_bond_only_issuer_without_parent_gics(df: pl.DataFrame) -> pl.DataFrame:
    out = df
    assert "parent_gics_sub_industry_code" in out.columns, (
        f"parent_gics_sub_industry_code column missing: {out.columns}"
    )
    fallback_rows = out.filter(
        (~pl.col("has_issued_equity")) & pl.col("parent_gics_sub_industry_code").is_null()
    )
    assert fallback_rows["gics_sub_industry_code"].null_count() == 0, (
        f"bond-only issuers without parent GICS have null gics_sub_industry_code: count={fallback_rows.height}"
    )
    return out
