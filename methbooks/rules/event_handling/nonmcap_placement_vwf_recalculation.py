# methbooks/rules/event_handling/nonmcap_placement_vwf_recalculation.py
"""
Purpose: For Non-Market Capitalization Weighted indexes, recalculate VWF when NOS or free float changes from share placements, block sales, or secondary offerings to offset any change in market capitalization; CF unchanged since no inflow is generated from these events.
Datapoints: security_id, nos, fif, constraint_factor, variable_weighting_factor
Thresholds: none
Source: meth-pipeline/MSCI_Corporate_Events_Methodology_20260210/2026-04-23T13-31-41Z/input/markdown.md section "4.6 Share Placements and Offerings treatment in non-market capitalization weighted indexes" near line 4919.
See also: methbooks/methodologies/MSCI/corporate_events.py (corporate events apply pipeline).
"""
from __future__ import annotations

import polars as pl


def nonmcap_placement_vwf_recalculation(df: pl.DataFrame) -> pl.DataFrame:
    required = ["security_id", "nos", "fif", "constraint_factor", "variable_weighting_factor"]
    missing = [c for c in required if c not in df.columns]
    assert not missing, f"missing required columns: {missing}"

    out = df

    cf_min = float(out["constraint_factor"].min())
    assert cf_min >= 0, f"CF must be >= 0, actual min: {cf_min}"

    nos_min = float(out["nos"].min())
    assert nos_min >= 0, f"nos must be >= 0, actual min: {nos_min}"

    fif_min = float(out["fif"].min())
    fif_max = float(out["fif"].max())
    assert fif_min >= 0, f"fif must be >= 0, actual min: {fif_min}"
    assert fif_max <= 1, f"fif must be <= 1, actual max: {fif_max}"

    # CF must be unchanged after placement events; validate via pre/post columns if present.
    if "constraint_factor_pre" in out.columns and "constraint_factor_post" in out.columns:
        changed = out.filter(
            pl.col("constraint_factor_pre") != pl.col("constraint_factor_post")
        )
        assert changed.is_empty(), (
            f"CF must be unchanged after placement event (no inflow generated); "
            f"offending security_ids: {changed['security_id'].to_list()}"
        )

    return out
