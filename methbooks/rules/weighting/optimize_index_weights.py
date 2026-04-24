"""
Purpose: Determine constituent weights via Barra Open Optimizer (GEMLTL model), minimizing ex-ante tracking error vs Parent Index subject to climate constraints (GHG intensity reduction vs Reference Index, 7% annual decarbonization trajectory, high-climate-impact sector active weight floor) and diversification constraints (constituent/sector/country active weight bounds, security weight multiple, turnover cap, factor and specific risk aversion). On infeasibility, relax one-way turnover in 1% steps up to 20% and active sector weight in 1% steps up to +/-20% alternately; if still infeasible, Index is not rebalanced.
Datapoints: ghg_intensity_scope123, parent_index_weight, reference_index_weight, high_climate_impact_sector.
Thresholds: ctb_ghg_intensity_reduction_pct=30, pab_ghg_intensity_reduction_pct=50, annual_decarbonization_rate=0.07, high_climate_impact_sector_active_weight_floor_pct=0, constituent_active_weight_pct=2, security_weight_max_multiple_of_parent=20, active_sector_weight_pct=5, active_country_weight_pct=5, one_way_turnover_pct=5, common_factor_risk_aversion=0.0075, specific_risk_aversion=0.075.
Source: methbooks/data/markdown/MSCI_EU_CTB_PAB_Overlay_Indexes_Methodology_20260211.md section "2.5 Optimization Constraints" near line 334.
See also: methbooks/rules/weighting/renormalize_parent_weights.py (simpler pro-rata weighting for non-optimized indexes).
"""
from __future__ import annotations
import polars as pl

CTB_GHG_INTENSITY_REDUCTION_PCT = 30
PAB_GHG_INTENSITY_REDUCTION_PCT = 50
ANNUAL_DECARBONIZATION_RATE = 0.07
HIGH_CLIMATE_IMPACT_SECTOR_ACTIVE_WEIGHT_FLOOR_PCT = 0
CONSTITUENT_ACTIVE_WEIGHT_PCT = 2
SECURITY_WEIGHT_MAX_MULTIPLE_OF_PARENT = 20
ACTIVE_SECTOR_WEIGHT_PCT = 5
ACTIVE_COUNTRY_WEIGHT_PCT = 5
ONE_WAY_TURNOVER_PCT = 5
COMMON_FACTOR_RISK_AVERSION = 0.0075
SPECIFIC_RISK_AVERSION = 0.075
TURNOVER_RELAXATION_STEP_PCT = 1
TURNOVER_MAX_RELAXED_PCT = 20
SECTOR_WEIGHT_RELAXATION_STEP_PCT = 1
SECTOR_WEIGHT_MAX_RELAXED_PCT = 20


def optimize_index_weights(df: pl.DataFrame) -> pl.DataFrame:
    """Proxy for Barra optimizer: normalizes parent_index_weight to sum to 1.

    In production, this is replaced by a call to the Barra Open Optimizer
    (GEMLTL model). In the mock pipeline, parent_index_weight is used as
    a stand-in for optimizer output weights.
    """
    total = df["parent_index_weight"].sum()
    out = df.with_columns(
        (pl.col("parent_index_weight") / total).alias("weight")
    )
    assert "weight" in out.columns, f"weight column missing after optimization: {out.columns}"
    assert abs(float(out["weight"].sum()) - 1.0) < 1e-6, f"Weights do not sum to 1: sum={out['weight'].sum()}"
    assert (out["weight"] >= 0).all(), f"Negative weight in optimizer output: min={out['weight'].min()}"
    return out
