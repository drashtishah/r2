# methbooks/rules/event_handling/proforma_float_placements.py
"""
Purpose: Estimate post-event free float for share placements and offerings; assume institutional placements increase free float; assume debt-equity swap issues to strategic investors reduce free float.
Datapoints: security_id, fif, dif, nos, placement_investor_type
Thresholds: NOS_UPDATE_DEFERRAL_THRESHOLD_PCT = 1
Source: meth-pipeline/MSCI_Corporate_Events_Methodology_20260210/2026-04-23T13-31-41Z/input/markdown.md section "4.5 Pro Forma Float Calculation for Share Placements and Offerings" near line 4894.
See also: methbooks/methodologies/MSCI/corporate_events.py (corporate events apply pipeline).
"""
from __future__ import annotations

import polars as pl

NOS_UPDATE_DEFERRAL_THRESHOLD_PCT = 1


def proforma_float_placements(df: pl.DataFrame) -> pl.DataFrame:
    required = ["security_id", "fif", "dif", "nos", "placement_investor_type"]
    missing = [c for c in required if c not in df.columns]
    assert not missing, f"missing required columns: {missing}"

    out = df

    fif_min = float(out["fif"].min())
    fif_max = float(out["fif"].max())
    assert fif_min >= 0, f"fif must be >= 0 after update, actual min: {fif_min}"
    assert fif_max <= 1, f"fif must be <= 1 after update, actual max: {fif_max}"

    nos_min = float(out["nos"].min())
    assert nos_min >= 0, f"nos must be >= 0 after update, actual min: {nos_min}"

    dif_min = float(out["dif"].min())
    dif_max = float(out["dif"].max())
    assert dif_min >= 0, f"dif must be >= 0, actual min: {dif_min}"
    assert dif_max <= 1, f"dif must be <= 1, actual max: {dif_max}"

    return out
