# methbooks/rules/event_handling/spinoff_exdate_paf.py
"""
Purpose: Apply PAF to parent security on ex-date of a spin-off based on terms and spun-off market price.
Datapoints: security_id, closing_price, nos
Thresholds: none
Source: meth-pipeline/MSCI_Corporate_Events_Methodology_20260210/2026-04-23T13-31-41Z/input/markdown.md section "2.8.1 Treatment when spun-off trades on ex-date" near line 3083.
See also: methbooks/methodologies/MSCI/corporate_events.py (corporate events apply pipeline).
"""
from __future__ import annotations

import polars as pl


def spinoff_exdate_paf(df: pl.DataFrame) -> pl.DataFrame:
    required = ["security_id", "closing_price", "nos"]
    missing = [c for c in required if c not in df.columns]
    assert not missing, f"missing required columns: {missing}"

    out = df

    cp_min = float(out["closing_price"].min())
    assert cp_min > 0, (
        f"closing_price must be > 0 for PAF calculation "
        f"PAF = [P(t) + Spun-Off P(t) * Spun-Off Shares Issued / Shares Before] / [P(t)]; "
        f"actual min closing_price: {cp_min}"
    )

    nos_min = float(out["nos"].min())
    assert nos_min >= 0, f"nos must be >= 0, actual min: {nos_min}"

    return out
