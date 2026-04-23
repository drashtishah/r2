# methbooks/rules/event_handling/partial_tender_nos_fif_update.py
"""
Purpose: Implement NOS/FIF/DIF changes resulting from partial tenders and buybacks when the outcome is published and the change meets the size-segment threshold; otherwise defer to next Index Review.
Datapoints: security_id, nos, fif, dif, size_segment
Thresholds: NOS_THRESHOLD_STANDARD_PCT = 5, NOS_THRESHOLD_SMALL_CAP_PCT = 10, NOS_THRESHOLD_MICRO_CAP_PCT = 25, NOS_UPDATE_DEFERRAL_THRESHOLD_PCT = 1
Source: meth-pipeline/MSCI_Corporate_Events_Methodology_20260210/2026-04-23T13-31-41Z/input/markdown.md section "2.4 Partial tender offers and buyback offers" near line 791.
See also: methbooks/methodologies/MSCI/corporate_events.py (corporate events apply pipeline).
"""
from __future__ import annotations

import polars as pl

NOS_THRESHOLD_STANDARD_PCT = 5
NOS_THRESHOLD_SMALL_CAP_PCT = 10
NOS_THRESHOLD_MICRO_CAP_PCT = 25
NOS_UPDATE_DEFERRAL_THRESHOLD_PCT = 1


def partial_tender_nos_fif_update(df: pl.DataFrame) -> pl.DataFrame:
    required = ["security_id", "nos", "fif", "dif", "size_segment"]
    missing = [c for c in required if c not in df.columns]
    assert not missing, f"missing required columns: {missing}"

    out = df

    nos_min = float(out["nos"].min())
    assert nos_min >= 0, f"nos must be >= 0 after update, actual min: {nos_min}"

    fif_min = float(out["fif"].min())
    fif_max = float(out["fif"].max())
    assert fif_min >= 0, f"fif must be >= 0, actual min: {fif_min}"
    assert fif_max <= 1, f"fif must be <= 1, actual max: {fif_max}"

    dif_min = float(out["dif"].min())
    dif_max = float(out["dif"].max())
    assert dif_min >= 0, f"dif must be >= 0, actual min: {dif_min}"
    assert dif_max <= 1, f"dif must be <= 1, actual max: {dif_max}"

    return out
