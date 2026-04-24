"""
Purpose: Exclude securities whose Foreign Inclusion Factor is below 0.15 unless the
company is very large and exclusion would compromise Standard Index representation
of the market.
Datapoints: fif, is_existing_imi_constituent, ff_adjusted_mcap_usd,
  standard_index_interim_cutoff_usd.
Thresholds: MIN_FIF=0.15,
  LOW_FIF_STANDARD_OVERRIDE_FF_MCAP_MULTIPLIER=1.8,
  LOW_FIF_STANDARD_OVERRIDE_HALF_CUTOFF_MULTIPLIER=0.5.
Source: meth-pipeline/MSCI_Global_Investable_Market_Indexes_Methodology_20260331/2026-04-24T07-46-35Z/input/markdown.md section "2.2.6 Global Minimum Foreign Inclusion Factor Requirement" near line 1198.
See also: methbooks/rules/eligibility/apply_minimum_foreign_room_requirement.py (foreign room screen).
"""
from __future__ import annotations

import polars as pl

MIN_FIF = 0.15
LOW_FIF_STANDARD_OVERRIDE_FF_MCAP_MULTIPLIER = 1.8
LOW_FIF_STANDARD_OVERRIDE_HALF_CUTOFF_MULTIPLIER = 0.5


def apply_global_minimum_fif_requirement(df: pl.DataFrame) -> pl.DataFrame:
    assert "fif" in df.columns, f"fif column missing: {df.columns}"
    override_threshold = (
        LOW_FIF_STANDARD_OVERRIDE_FF_MCAP_MULTIPLIER
        * LOW_FIF_STANDARD_OVERRIDE_HALF_CUTOFF_MULTIPLIER
        * pl.col("standard_index_interim_cutoff_usd")
    )
    passes = (
        pl.col("is_existing_imi_constituent")
        | (pl.col("fif") >= MIN_FIF)
        | (pl.col("ff_adjusted_mcap_usd") >= override_threshold)
    )
    out = df.filter(passes)
    bad = out.filter(
        (~pl.col("is_existing_imi_constituent"))
        & (pl.col("fif") < MIN_FIF)
        & (pl.col("ff_adjusted_mcap_usd") < override_threshold)
    )
    assert bad.height == 0, (
        f"non-constituent with fif < {MIN_FIF} not meeting large-company override"
        f" survived: {bad.height} rows; min fif={bad['fif'].min()}"
    )
    return out
