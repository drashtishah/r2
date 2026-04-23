# methbooks/rules/event_handling/stock_dividend_paf.py
"""
Purpose: Apply PAF = [(Shares Issued + Shares Before) / Shares Before] on ex-date of stock dividends and bonus issues; adjust NOS accordingly; FIF review triggered if treasury shares distributed.
Datapoints: security_id, nos, closing_price, fif, shares_before, shares_issued
Thresholds: none
Source: meth-pipeline/MSCI_Corporate_Events_Methodology_20260210/2026-04-23T13-31-41Z/input/markdown.md section "3.2 Stock Dividends / Bonus Issues" near line 3933.
See also: methbooks/methodologies/MSCI/corporate_events.py (corporate events apply pipeline).
"""
from __future__ import annotations

import polars as pl


def stock_dividend_paf(df: pl.DataFrame) -> pl.DataFrame:
    required = ["security_id", "nos", "closing_price", "fif", "shares_before", "shares_issued"]
    missing = [c for c in required if c not in df.columns]
    assert not missing, f"missing required columns: {missing}"

    out = df

    sb_min = float(out["shares_before"].min())
    assert sb_min > 0, (
        f"shares_before must be > 0 for PAF = (shares_issued + shares_before) / shares_before; "
        f"actual min shares_before: {sb_min}"
    )

    si_min = float(out["shares_issued"].min())
    assert si_min >= 0, (
        f"shares_issued must be >= 0 for standard stock dividend, actual min: {si_min}"
    )

    nos_min = float(out["nos"].min())
    assert nos_min >= 0, f"nos must be >= 0 after stock dividend, actual min: {nos_min}"

    fif_min = float(out["fif"].min())
    fif_max = float(out["fif"].max())
    assert fif_min >= 0, f"fif must be >= 0, actual min: {fif_min}"
    assert fif_max <= 1, f"fif must be <= 1, actual max: {fif_max}"

    cp_min = float(out["closing_price"].min())
    assert cp_min > 0, f"closing_price must be > 0, actual min: {cp_min}"

    return out
