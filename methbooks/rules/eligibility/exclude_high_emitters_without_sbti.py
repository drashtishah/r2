"""
Purpose: Exclude high-emission-intensity securities and high potential-carbon-emission
    fossil fuel companies that have not received approved Science-Based Targets initiative
    (SBTi) approval. Also exclude securities without emission intensity data.
Datapoints: has_emission_intensity_data, emission_intensity, fossil_fuel_reserves_energy_application,
    total_potential_carbon_emissions_ex_met_coal, approved_sbti.
Thresholds: 95th percentile of emission_intensity (computed from df); 95th percentile of
    total_potential_carbon_emissions_ex_met_coal among fossil fuel reserves companies (computed from df).
Source: methbooks/data/markdown/MSCI_Climate_Action_Indexes_Methodology_20251211.md section "Eligibility Criteria" near line 1.
See also: methbooks/rules/eligibility/exclude_bottom_quartile_climate_risk_management.py (subsequent eligibility step).
"""
from __future__ import annotations

import polars as pl

EMISSION_INTENSITY_PERCENTILE = 0.95
POTENTIAL_CARBON_PERCENTILE = 0.95


def exclude_high_emitters_without_sbti(df: pl.DataFrame) -> pl.DataFrame:
    # Compute 95th percentile thresholds from df
    intensity_vals = df.filter(pl.col("has_emission_intensity_data")).select(
        pl.col("emission_intensity").quantile(EMISSION_INTENSITY_PERCENTILE)
    ).item()

    ff_rows = df.filter(pl.col("fossil_fuel_reserves_energy_application"))
    if ff_rows.height > 0:
        potential_threshold = ff_rows.select(
            pl.col("total_potential_carbon_emissions_ex_met_coal").quantile(POTENTIAL_CARBON_PERCENTILE)
        ).item()
    else:
        potential_threshold = float("inf")

    # Exclude: no emission intensity data, OR (high emitter OR high potential carbon) AND no SBTi
    out = df.filter(
        pl.col("has_emission_intensity_data")
        & ~(
            (
                (pl.col("emission_intensity") >= intensity_vals)
                | (
                    pl.col("fossil_fuel_reserves_energy_application")
                    & (pl.col("total_potential_carbon_emissions_ex_met_coal") >= potential_threshold)
                )
            )
            & ~pl.col("approved_sbti")
        )
    )
    assert "has_emission_intensity_data" in out.columns, (
        f"has_emission_intensity_data column missing: {out.columns}"
    )
    assert out["has_emission_intensity_data"].all(), (
        f"rows without emission intensity data survived"
    )
    return out
