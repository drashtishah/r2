"""
Purpose: Assign GICS to a special purpose vehicle or captive finance vehicle based on the business of its related entities rather than its own minimal operations.
Datapoints: is_spv_or_captive_finance, related_entity_gics_sub_industry_code.
Thresholds: none.
Source: methbooks/data/markdown/MSCI_Global_Industry_Classification_Standard_(GICS®)_Methodology_20250220.md section "Section 8.1.2: Classification of a company that has issued only corporate bonds" near line 3885.
See also: methbooks/rules/eligibility/classify_bond_only_issuer_by_parent_gics.py.
"""
from __future__ import annotations

import polars as pl


def classify_spv_by_related_entity(df: pl.DataFrame) -> pl.DataFrame:
    out = df.with_columns(
        pl.when(pl.col("is_spv_or_captive_finance"))
        .then(pl.col("related_entity_gics_sub_industry_code"))
        .otherwise(pl.col("gics_sub_industry_code"))
        .alias("gics_sub_industry_code")
    )
    assert out["is_spv_or_captive_finance"].dtype == pl.Boolean, (
        f"is_spv_or_captive_finance must be Boolean, got {out['is_spv_or_captive_finance'].dtype}"
    )
    spv_rows = out.filter(pl.col("is_spv_or_captive_finance"))
    assert spv_rows["related_entity_gics_sub_industry_code"].null_count() == 0, (
        f"SPV rows have null related_entity_gics_sub_industry_code: count={spv_rows.height}"
    )
    assert (spv_rows["gics_sub_industry_code"] == spv_rows["related_entity_gics_sub_industry_code"]).all(), (
        f"SPV gics_sub_industry_code does not match related_entity_gics_sub_industry_code: "
        f"offending_count={spv_rows.filter(pl.col('gics_sub_industry_code') != pl.col('related_entity_gics_sub_industry_code')).height}"
    )
    return out
