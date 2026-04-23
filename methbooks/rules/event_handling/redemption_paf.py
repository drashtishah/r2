# methbooks/rules/event_handling/redemption_paf.py
"""
Purpose: Apply PAF adjusting for redemption consideration on ex-date of mandatory redemptions; decrease NOS as of close of ex-date; free float unchanged. Treat optional redemptions as partial buybacks.
Datapoints: security_id, nos, closing_price, offer_price, shares_acquired
Thresholds: none
Source: meth-pipeline/MSCI_Corporate_Events_Methodology_20260210/2026-04-23T13-31-41Z/input/markdown.md section "3.7 Redemptions" near line 4635.
See also: methbooks/methodologies/MSCI/corporate_events.py (corporate events apply pipeline).
"""
from __future__ import annotations

import polars as pl


def redemption_paf(df: pl.DataFrame) -> pl.DataFrame:
    required = ["security_id", "nos", "closing_price", "offer_price", "shares_acquired"]
    missing = [c for c in required if c not in df.columns]
    assert not missing, f"missing required columns: {missing}"

    out = df

    cp_min = float(out["closing_price"].min())
    assert cp_min > 0, (
        f"closing_price must be > 0 for "
        f"PAF = [((Shares Before - Shares Acquired) * P(t) + Shares Acquired * Offer P) / Shares Before] / [P(t)]; "
        f"actual min closing_price: {cp_min}"
    )

    op_min = float(out["offer_price"].min())
    assert op_min > 0, (
        f"offer_price must be > 0 for PAF calculation, actual min: {op_min}"
    )

    nos_min = float(out["nos"].min())
    assert nos_min >= 0, f"nos must be >= 0 after redemption, actual min: {nos_min}"

    sa_min = float(out["shares_acquired"].min())
    assert sa_min >= 0, f"shares_acquired must be >= 0, actual min: {sa_min}"

    return out
