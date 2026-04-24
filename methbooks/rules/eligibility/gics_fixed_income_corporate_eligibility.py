"""
Purpose: Restrict fixed income GICS classification to corporate bond issuers; exclude supranationals, municipals, and sovereigns from fixed income coverage.
Datapoints: fi_entity_type, has_issued_corporate_bonds.
Thresholds: none.
Source: methbooks/data/markdown/MSCI_Global_Industry_Classification_Standard_(GICS®)_Methodology_20250220.md section "Section 8.1: Fixed income universe eligible for classification" near line 3848.
See also: methbooks/rules/eligibility/gics_equity_issuer_eligibility.py.
"""
from __future__ import annotations

import polars as pl

_INELIGIBLE_FI_ENTITY_TYPES = ["supranational", "municipal", "sovereign"]


def gics_fixed_income_corporate_eligibility(df: pl.DataFrame) -> pl.DataFrame:
    out = df.filter(
        (pl.col("fi_entity_type") == "corporate") & (pl.col("has_issued_corporate_bonds"))
    )
    assert "fi_entity_type" in out.columns, f"fi_entity_type column missing: {out.columns}"
    assert not out.filter(pl.col("fi_entity_type").is_in(_INELIGIBLE_FI_ENTITY_TYPES)).height > 0, (
        f"ineligible fi_entity_type survived: {out['fi_entity_type'].unique().to_list()}"
    )
    assert out["has_issued_corporate_bonds"].all(), (
        "rows with has_issued_corporate_bonds == False survived FI eligibility filter"
    )
    return out
