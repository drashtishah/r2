"""
Purpose: Restrict GICS classification to equity-issuing companies and exclude non-corporate entity types.
Datapoints: entity_type, is_equity_issuer, is_subsidiary_with_separate_financials.
Thresholds: none.
Source: methbooks/data/markdown/MSCI_Global_Industry_Classification_Standard_(GICS®)_Methodology_20250220.md section "Section 2: Companies Eligible for GICS Classification" near line 762.
See also: methbooks/rules/eligibility/gics_fixed_income_corporate_eligibility.py.
"""
from __future__ import annotations

import polars as pl

_INELIGIBLE_ENTITY_TYPES = {"supranational", "municipal", "sovereign", "shell_company", "mutual_fund", "etf"}


def gics_equity_issuer_eligibility(df: pl.DataFrame) -> pl.DataFrame:
    out = df.filter(
        (pl.col("entity_type") == "corporate") & (pl.col("is_equity_issuer"))
    )
    assert "entity_type" in out.columns, f"entity_type column missing: {out.columns}"
    assert "is_equity_issuer" in out.columns, f"is_equity_issuer column missing: {out.columns}"
    assert not out["entity_type"].is_in(list(_INELIGIBLE_ENTITY_TYPES)).any(), (
        f"ineligible entity_type survived: {out['entity_type'].unique().to_list()}"
    )
    assert out["is_equity_issuer"].all(), "rows with is_equity_issuer == False survived eligibility filter"
    return out
