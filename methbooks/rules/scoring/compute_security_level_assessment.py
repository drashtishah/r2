"""
Purpose: Compute security-level assessment score (1-4) combining intensity, green business,
    climate risk management scores and SBTi/track record status.
Datapoints: approved_sbti, credible_track_record, intensity_score, climate_risk_management_score,
    green_business_score, green_business_revenue_pct.
Thresholds: green_business_revenue_pct >= 5.0 for Tier 2 green business condition.
Source: methbooks/data/markdown/MSCI_Climate_Action_Indexes_Methodology_20251211.md section "2.3.1 Security-Level Assessment" near line 237.
See also: methbooks/rules/ranking/rank_by_assessment_then_mcap.py (uses this output).
"""
from __future__ import annotations

import polars as pl

GREEN_BUSINESS_REVENUE_THRESHOLD = 5.0


def compute_security_level_assessment(df: pl.DataFrame) -> pl.DataFrame:
    out = df.with_columns(
        pl.when(
            pl.col("approved_sbti") | (pl.col("credible_track_record") == True)  # noqa: E712
        )
        .then(
            (pl.col("intensity_score") - 2).clip(lower_bound=1)
        )
        .when(
            (pl.col("climate_risk_management_score") == 4)
            | (
                (pl.col("green_business_score") == 4)
                & (pl.col("green_business_revenue_pct") >= GREEN_BUSINESS_REVENUE_THRESHOLD)
            )
        )
        .then(
            (pl.col("intensity_score") - 1).clip(lower_bound=1)
        )
        .otherwise(pl.col("intensity_score"))
        .cast(pl.Int32)
        .alias("security_level_assessment")
    )
    assert "security_level_assessment" in out.columns, (
        f"security_level_assessment column missing: {out.columns}"
    )
    assert out["security_level_assessment"].min() >= 1, (
        f"security_level_assessment below 1: {out['security_level_assessment'].min()}"
    )
    assert out["security_level_assessment"].max() <= 4, (
        f"security_level_assessment above 4: {out['security_level_assessment'].max()}"
    )
    return out
