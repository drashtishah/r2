# methbooks/rules/event_handling/sunday_intermediary_index_events.py
"""
Purpose: For MSCI Indexes calculated on Sundays (Monday-Friday base calendar): apply PAF to both Sunday and Monday when a corporate event requires PAF on Sunday; implement NOS/FIF/DIF changes as of close of Monday.
Datapoints: security_id, nos, fif, dif, trading_calendar_type
Thresholds: none
Source: meth-pipeline/MSCI_Corporate_Events_Methodology_20260210/2026-04-23T13-31-41Z/input/markdown.md section "Appendix IV: Corporate Events Implementation in Sunday Intermediary Index Calculation" near line 5892.
See also: methbooks/methodologies/MSCI/corporate_events.py (corporate events apply pipeline).
"""
from __future__ import annotations

import polars as pl

_SUNDAY_INTERMEDIARY_CALENDAR = "sunday_intermediary"


def sunday_intermediary_index_events(df: pl.DataFrame) -> pl.DataFrame:
    required = ["security_id", "nos", "fif", "dif", "trading_calendar_type"]
    missing = [c for c in required if c not in df.columns]
    assert not missing, f"missing required columns: {missing}"

    out = df

    assert "trading_calendar_type" in out.columns, (
        "PAF applied Sunday and Monday; NOS/FIF/DIF changes effective as of close of Monday; "
        "trading_calendar_type column must be present"
    )

    nos_min = float(out["nos"].min())
    assert nos_min >= 0, f"nos must be >= 0, actual min: {nos_min}"

    fif_min = float(out["fif"].min())
    fif_max = float(out["fif"].max())
    assert fif_min >= 0, f"fif must be >= 0, actual min: {fif_min}"
    assert fif_max <= 1, f"fif must be <= 1, actual max: {fif_max}"

    dif_min = float(out["dif"].min())
    dif_max = float(out["dif"].max())
    assert dif_min >= 0, f"dif must be >= 0, actual min: {dif_min}"
    assert dif_max <= 1, f"dif must be <= 1, actual max: {dif_max}"

    return out
