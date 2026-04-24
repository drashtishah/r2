"""
Purpose: Assign the company's GICS classification to all its equity securities, including ADRs and GDRs, so all securities from the same company share a single classification.
Datapoints: company_id, gics_sub_industry_code.
Thresholds: none.
Source: methbooks/data/markdown/MSCI_Global_Industry_Classification_Standard_(GICS®)_Methodology_20250220.md section "Section 3.2: Propagation of GICS from company level to security level" near line 815.
See also: methbooks/rules/maintenance/propagate_gics_equity_to_corporate_bonds.py.
"""
from __future__ import annotations

import polars as pl


def propagate_gics_from_company_to_equity_securities(df: pl.DataFrame) -> pl.DataFrame:
    company_codes = df.group_by("company_id").agg(
        pl.col("gics_sub_industry_code").first().alias("company_code")
    )
    out = (
        df.join(company_codes, on="company_id", how="left")
        .with_columns(pl.col("company_code").alias("gics_sub_industry_code"))
        .drop("company_code")
    )
    assert "company_id" in out.columns, f"company_id column missing: {out.columns}"
    assert "gics_sub_industry_code" in out.columns, f"gics_sub_industry_code column missing: {out.columns}"
    assert out["company_id"].null_count() == 0, "null company_id values found"
    assert out["gics_sub_industry_code"].null_count() == 0, "null gics_sub_industry_code after propagation"
    n_unique_per_company = out.group_by("company_id").agg(
        pl.col("gics_sub_industry_code").n_unique().alias("n_unique")
    )
    offenders = n_unique_per_company.filter(pl.col("n_unique") > 1)
    assert offenders.height == 0, (
        f"first offending company_id={offenders['company_id'][0] if offenders.height > 0 else None}"
    )
    return out
