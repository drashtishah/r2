# methbooks/rules/event_handling/nonmcap_vwf_recalculation_ma_spinoff.py
"""
Purpose: Recalculate VWF for Non-Market Capitalization Weighted indexes after M&A and spin-off events so post-event market cap equals the pre-event market cap.
Datapoints: security_id, variable_weighting_factor, constraint_factor
Thresholds: VWF_CAPPED_WEIGHTED_INDEX = 1
Source: meth-pipeline/MSCI_Corporate_Events_Methodology_20260210/2026-04-23T13-31-41Z/input/markdown.md section "2.7 M&A treatment in capped weighted and non-market capitalization weighted indexes" near line 963.
See also: methbooks/methodologies/MSCI/corporate_events.py (corporate events apply pipeline).
"""
from __future__ import annotations

import polars as pl

VWF_CAPPED_WEIGHTED_INDEX = 1


def nonmcap_vwf_recalculation_ma_spinoff(df: pl.DataFrame) -> pl.DataFrame:
    required = ["security_id", "variable_weighting_factor", "constraint_factor"]
    missing = [c for c in required if c not in df.columns]
    assert not missing, f"missing required columns: {missing}"

    out = df

    assert VWF_CAPPED_WEIGHTED_INDEX == 1, (
        f"VWF for Capped Weighted Index constituents must equal 1, "
        f"actual: {VWF_CAPPED_WEIGHTED_INDEX}"
    )

    if "is_capped_weighted_index" in out.columns:
        capped_rows = out.filter(pl.col("is_capped_weighted_index"))
        invalid = capped_rows.filter(pl.col("variable_weighting_factor") != VWF_CAPPED_WEIGHTED_INDEX)
        assert invalid.is_empty(), (
            f"VWF must = {VWF_CAPPED_WEIGHTED_INDEX} for all Capped Weighted Index constituents; "
            f"offending variable_weighting_factor values: {invalid['variable_weighting_factor'].to_list()}"
        )

    cf_min = float(out["constraint_factor"].min())
    assert cf_min >= 0, f"constraint_factor must be >= 0, actual min: {cf_min}"

    return out
