"""
Purpose: Exclude companies with BISR-assessed involvement in the value chain of any
    of the seven controversial weapon categories (cluster bombs, landmines, depleted
    uranium weapons, chemical and biological weapons, blinding laser weapons,
    non-detectable fragments, incendiary weapons). The flag covers direct production,
    component production, delivery-platform production, 50%+ ownership by a
    manufacturer, and significant-stake ownership of a manufacturer.
Datapoints: bisr_controversial_weapons_flag.
Thresholds: none.
Source: meth-pipeline/MSCI_Global_ex_Controversial_Weapons_Indexes_Methodology_20251211/2026-04-23T20-59-13Z/input/markdown.md section "2.2 Eligibility Criteria" near line 87.
See also: methbooks/rules/eligibility/exclude_unrated_capital_goods_materials.py (companion exclusion for unrated companies in specific GICS groups).
"""
from __future__ import annotations

import polars as pl


def exclude_controversial_weapons_involvement(df: pl.DataFrame) -> pl.DataFrame:
    out = df.filter(pl.col("bisr_controversial_weapons_flag") == False)  # noqa: E712
    assert "bisr_controversial_weapons_flag" in out.columns, (
        f"bisr_controversial_weapons_flag column missing after filter: {out.columns}"
    )
    assert out["bisr_controversial_weapons_flag"].sum() == 0, (
        f"rows with bisr_controversial_weapons_flag == True survived: "
        f"{out['bisr_controversial_weapons_flag'].sum()}"
    )
    return out
