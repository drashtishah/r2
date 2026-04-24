"""
Purpose: Assign companies significantly diversified across three or more sectors with no majority activity to Industrial Conglomerates or Multi-Sector Holdings.
Datapoints: num_distinct_active_sectors, primary_activity_revenue_pct.
Thresholds: MIN_SECTORS_FOR_CONGLOMERATE = 3.
Source: methbooks/data/markdown/MSCI_Global_Industry_Classification_Standard_(GICS®)_Methodology_20250220.md section "Section 3.1: Classification by revenue and earnings" near line 806.
See also: methbooks/rules/eligibility/classify_by_qualitative_research.py.
"""
from __future__ import annotations

import polars as pl

MIN_SECTORS_FOR_CONGLOMERATE = 3
CONGLOMERATE_CODES = {20105010, 40201030}


def classify_diversified_conglomerate(df: pl.DataFrame) -> pl.DataFrame:
    out = df.with_columns(
        pl.when(
            (pl.col("num_distinct_active_sectors") >= MIN_SECTORS_FOR_CONGLOMERATE)
            & (pl.col("primary_activity_revenue_pct") < 0.5)
        )
        .then(pl.lit(20105010))
        .otherwise(pl.col("gics_sub_industry_code"))
        .alias("gics_sub_industry_code")
    )
    assert "num_distinct_active_sectors" in out.columns, (
        f"num_distinct_active_sectors column missing: {out.columns}"
    )
    assert int(out["num_distinct_active_sectors"].min()) >= 0, (
        f"num_distinct_active_sectors contains negative values: min={out['num_distinct_active_sectors'].min()}"
    )
    conglomerate_rows = out.filter(
        (pl.col("num_distinct_active_sectors") >= MIN_SECTORS_FOR_CONGLOMERATE)
        & (pl.col("primary_activity_revenue_pct") < 0.5)
    )
    min_sectors = int(conglomerate_rows["num_distinct_active_sectors"].min()) if conglomerate_rows.height > 0 else MIN_SECTORS_FOR_CONGLOMERATE
    assert conglomerate_rows["gics_sub_industry_code"].is_in(list(CONGLOMERATE_CODES)).all(), (
        f"conglomerate rows have unexpected gics_sub_industry_code: min_sectors={min_sectors}"
    )
    return out
