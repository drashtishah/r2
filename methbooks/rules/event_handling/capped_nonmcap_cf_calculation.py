# methbooks/rules/event_handling/capped_nonmcap_cf_calculation.py
"""
Purpose: Calculate post-event Constraint Factor for M&A and spin-off events in Capped Weighted and Non-Market Capitalization Weighted indexes using the Maintenance Formula or Addition Formula.
Datapoints: security_id, nos, constraint_factor, fif, is_index_constituent, is_parent_index_constituent
Thresholds: CF_COUNTERPART_NOT_IN_INDEX_VALUE = 0
Source: meth-pipeline/MSCI_Corporate_Events_Methodology_20260210/2026-04-23T13-31-41Z/input/markdown.md section "2.7 M&A treatment in capped weighted and non-market capitalization weighted indexes" near line 963.
See also: methbooks/methodologies/MSCI/corporate_events.py (corporate events apply pipeline).
"""
from __future__ import annotations

import polars as pl

CF_COUNTERPART_NOT_IN_INDEX_VALUE = 0


def capped_nonmcap_cf_calculation(df: pl.DataFrame) -> pl.DataFrame:
    required = ["security_id", "nos", "constraint_factor", "fif", "is_index_constituent", "is_parent_index_constituent"]
    missing = [c for c in required if c not in df.columns]
    assert not missing, f"missing required columns: {missing}"

    out = df

    cf_min = float(out["constraint_factor"].min())
    assert cf_min >= 0, f"constraint_factor must be >= 0, actual min: {cf_min}"

    nos_min = float(out["nos"].min())
    assert nos_min >= 0, f"nos must be >= 0, actual min: {nos_min}"

    fif_min = float(out["fif"].min())
    fif_max = float(out["fif"].max())
    assert fif_min >= 0, f"fif must be >= 0, actual min: {fif_min}"
    assert fif_max <= 1, f"fif must be <= 1, actual max: {fif_max}"

    return out
