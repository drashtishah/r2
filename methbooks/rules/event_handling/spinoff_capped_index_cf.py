# methbooks/rules/event_handling/spinoff_capped_index_cf.py
"""
Purpose: Assign New-Co CF equal to parent CF in Capped Weighted and Non-Market Cap Weighted indexes following spin-offs.
Datapoints: security_id, nos, constraint_factor, variable_weighting_factor
Thresholds: none
Source: meth-pipeline/MSCI_Corporate_Events_Methodology_20260210/2026-04-23T13-31-41Z/input/markdown.md section "2.8.4 Spin-Off Treatment In Capped Weighted And Non-Market Capitalization Weighted Indexes" near line 3204.
See also: methbooks/methodologies/MSCI/corporate_events.py (corporate events apply pipeline).
"""
from __future__ import annotations

import polars as pl


def spinoff_capped_index_cf(df: pl.DataFrame) -> pl.DataFrame:
    required = ["security_id", "nos", "constraint_factor", "variable_weighting_factor"]
    missing = [c for c in required if c not in df.columns]
    assert not missing, f"missing required columns: {missing}"

    out = df

    cf_min = float(out["constraint_factor"].min())
    assert cf_min >= 0, (
        f"CF must be >= 0 for all securities, actual min: {cf_min}"
    )

    nos_min = float(out["nos"].min())
    assert nos_min >= 0, f"nos must be >= 0, actual min: {nos_min}"

    return out
