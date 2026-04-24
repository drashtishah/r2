"""
Purpose: Assign a bond-only issuer's GICS based on its parent or ultimate parent company's GICS classification as available in GICS Direct.
Datapoints: has_issued_equity, parent_gics_sub_industry_code.
Thresholds: none.
Source: methbooks/data/markdown/MSCI_Global_Industry_Classification_Standard_(GICS®)_Methodology_20250220.md section "Section 8.1.2: Classification of a company that has issued only corporate bonds" near line 3871.
See also: methbooks/rules/eligibility/classify_bond_only_issuer_without_parent_gics.py.
"""
from __future__ import annotations

import polars as pl


def classify_bond_only_issuer_by_parent_gics(df: pl.DataFrame) -> pl.DataFrame:
    out = df.with_columns(
        pl.when(
            (~pl.col("has_issued_equity")) & pl.col("parent_gics_sub_industry_code").is_not_null()
        )
        .then(pl.col("parent_gics_sub_industry_code"))
        .otherwise(pl.col("gics_sub_industry_code"))
        .alias("gics_sub_industry_code")
    )
    assert "parent_gics_sub_industry_code" in out.columns, (
        f"parent_gics_sub_industry_code column missing: {out.columns}"
    )
    bond_only_with_parent = out.filter(
        (~pl.col("has_issued_equity")) & pl.col("parent_gics_sub_industry_code").is_not_null()
    )
    assert (
        bond_only_with_parent["gics_sub_industry_code"] == bond_only_with_parent["parent_gics_sub_industry_code"]
    ).all(), "bond-only issuer with parent GICS does not have parent gics_sub_industry_code assigned"
    return out
