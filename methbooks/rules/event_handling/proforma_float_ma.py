# methbooks/rules/event_handling/proforma_float_ma.py
"""
Purpose: Estimate post-event free float on a pro forma basis for M&As: cash transactions leave acquirer float unchanged; stock-for-stock uses weighted average of pre-event free floats; mixed uses company-disclosed shareholder information or weighted average fallback.
Datapoints: security_id, fif, dif, nos, consideration_type
Thresholds: none
Source: meth-pipeline/MSCI_Corporate_Events_Methodology_20260210/2026-04-23T13-31-41Z/input/markdown.md section "2.6 Pro Forma Float Calculation for M&As" near line 910.
See also: methbooks/methodologies/MSCI/corporate_events.py (corporate events apply pipeline).
"""
from __future__ import annotations

import polars as pl


def proforma_float_ma(df: pl.DataFrame) -> pl.DataFrame:
    required = ["security_id", "fif", "dif", "nos", "consideration_type"]
    missing = [c for c in required if c not in df.columns]
    assert not missing, f"missing required columns: {missing}"

    out = df

    fif_min = float(out["fif"].min())
    fif_max = float(out["fif"].max())
    assert fif_min >= 0, f"fif must be >= 0 after pro forma calculation, actual min: {fif_min}"
    assert fif_max <= 1, f"fif must be <= 1 after pro forma calculation, actual max: {fif_max}"

    dif_min = float(out["dif"].min())
    dif_max = float(out["dif"].max())
    assert dif_min >= 0, f"dif must be >= 0 after pro forma calculation, actual min: {dif_min}"
    assert dif_max <= 1, f"dif must be <= 1 after pro forma calculation, actual max: {dif_max}"

    nos_min = float(out["nos"].min())
    assert nos_min >= 0, f"nos must be >= 0 after pro forma calculation, actual min: {nos_min}"

    return out
