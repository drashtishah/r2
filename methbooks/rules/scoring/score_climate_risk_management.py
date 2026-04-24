"""
Purpose: Assign sector-relative quartile climate risk management score (1-4); score 4 = best CRM.
Datapoints: crm_weighted_avg_score, ff_mcap, gics_sector.
Thresholds: none (quartile boundaries computed per sector).
Source: methbooks/data/markdown/MSCI_Climate_Action_Indexes_Methodology_20251211.md section "Scoring" near line 1.
See also: methbooks/rules/eligibility/exclude_bottom_quartile_climate_risk_management.py (CRM used in eligibility too).
"""
from __future__ import annotations

import polars as pl


def score_climate_risk_management(df: pl.DataFrame) -> pl.DataFrame:
    # Higher crm_weighted_avg_score -> score 4. Descending rank; rank=1 -> score=4.
    out = (
        df.with_columns(
            pl.struct(["crm_weighted_avg_score", "ff_mcap"])
            .rank(method="ordinal", descending=True)
            .over("gics_sector")
            .alias("_rank")
        )
        .with_columns(
            pl.col("gics_sector").count().over("gics_sector").alias("_n")
        )
        .with_columns(
            (4 - (((pl.col("_rank") - 1) * 4) // pl.col("_n"))).cast(pl.Int32).alias("climate_risk_management_score")
        )
        .drop(["_rank", "_n"])
    )
    assert "climate_risk_management_score" in out.columns, (
        f"climate_risk_management_score column missing: {out.columns}"
    )
    assert out["climate_risk_management_score"].min() >= 1, (
        f"climate_risk_management_score below 1: {out['climate_risk_management_score'].min()}"
    )
    assert out["climate_risk_management_score"].max() <= 4, (
        f"climate_risk_management_score above 4: {out['climate_risk_management_score'].max()}"
    )
    return out
