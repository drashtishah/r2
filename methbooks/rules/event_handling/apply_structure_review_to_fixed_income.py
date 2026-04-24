"""
Purpose: Apply all GICS Sub-Industry definition changes resulting from a GICS Structure Review to fixed income company classifications without exception.
Datapoints: gics_sub_industry_code, structure_review_change_flag, has_issued_corporate_bonds.
Thresholds: none.
Source: methbooks/data/markdown/MSCI_Global_Industry_Classification_Standard_(GICS®)_Methodology_20250220.md section "Section 8.3.2: Impact of GICS structure review" near line 3920.
See also: methbooks/rules/maintenance/review_fixed_income_gics_annually.py.
"""
from __future__ import annotations

import polars as pl


def apply_structure_review_to_fixed_income(df: pl.DataFrame) -> pl.DataFrame:
    out = df
    assert "structure_review_change_flag" in out.columns, (
        f"structure_review_change_flag column missing: {out.columns}"
    )
    assert out["structure_review_change_flag"].dtype == pl.Boolean, (
        f"structure_review_change_flag must be Boolean, got {out['structure_review_change_flag'].dtype}"
    )
    review_fi_rows = out.filter(
        pl.col("structure_review_change_flag") & pl.col("has_issued_corporate_bonds")
    )
    assert review_fi_rows["gics_sub_industry_code"].null_count() == 0, (
        f"structure review FI rows have null gics_sub_industry_code: count={review_fi_rows.height}"
    )
    return out
