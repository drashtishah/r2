"""
Purpose: Assign a company's existing equity GICS classification to its corporate bonds when the company has also issued equity securities.
Datapoints: company_id, has_issued_equity, gics_sub_industry_code.
Thresholds: none.
Source: methbooks/data/markdown/MSCI_Global_Industry_Classification_Standard_(GICS®)_Methodology_20250220.md section "Section 8.1.1: Classification of a company that has issued both equity and corporate bonds" near line 3858.
See also: methbooks/rules/maintenance/propagate_gics_from_company_to_equity_securities.py.
"""
from __future__ import annotations

import polars as pl


def propagate_gics_equity_to_corporate_bonds(df: pl.DataFrame) -> pl.DataFrame:
    out = df
    assert "company_id" in out.columns, f"company_id column missing: {out.columns}"
    assert "has_issued_equity" in out.columns, f"has_issued_equity column missing: {out.columns}"
    assert "gics_sub_industry_code" in out.columns, f"gics_sub_industry_code column missing: {out.columns}"
    equity_rows = out.filter(pl.col("has_issued_equity"))
    assert equity_rows["gics_sub_industry_code"].null_count() == 0, (
        "equity issuer rows have null gics_sub_industry_code"
    )
    n_unique = equity_rows.group_by("company_id").agg(
        pl.col("gics_sub_industry_code").n_unique().alias("n")
    )
    offenders = n_unique.filter(pl.col("n") > 1)
    assert offenders.height == 0, (
        f"first offending company_id={offenders['company_id'][0] if offenders.height > 0 else None}"
    )
    return out
