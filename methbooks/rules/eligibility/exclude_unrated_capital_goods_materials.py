"""
Purpose: Exclude companies in GICS Industry Groups Capital Goods or Materials that
    have not been assessed by MSCI Solutions on BISR data.
Datapoints: bisr_assessed, gics_industry_group.
Thresholds: EXCLUDED_GROUPS = ['Capital Goods', 'Materials'] (module constant).
Source: meth-pipeline/MSCI_Global_ex_Controversial_Weapons_Indexes_Methodology_20251211/2026-04-23T20-59-13Z/input/markdown.md section "2.3 Treatment of Unrated Companies" near line 108.
See also: methbooks/rules/eligibility/exclude_controversial_weapons_involvement.py (primary BISR exclusion applied before this rule).
"""
from __future__ import annotations

import polars as pl

EXCLUDED_GROUPS = ["Capital Goods", "Materials"]


def exclude_unrated_capital_goods_materials(df: pl.DataFrame) -> pl.DataFrame:
    out = df.filter(
        ~(
            (pl.col("bisr_assessed") == False)  # noqa: E712
            & pl.col("gics_industry_group").is_in(EXCLUDED_GROUPS)
        )
    )
    assert "bisr_assessed" in out.columns, (
        f"bisr_assessed column missing after filter: {out.columns}"
    )
    assert "gics_industry_group" in out.columns, (
        f"gics_industry_group column missing after filter: {out.columns}"
    )
    survivors = out.filter(
        (pl.col("bisr_assessed") == False)  # noqa: E712
        & pl.col("gics_industry_group").is_in(EXCLUDED_GROUPS)
    )
    assert survivors.height == 0, (
        f"unrated Capital Goods/Materials rows survived: {survivors.height} rows, "
        f"groups={survivors['gics_industry_group'].unique().to_list()}"
    )
    return out
