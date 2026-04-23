# methbooks/rules/event_handling/suspension_price_carryforward.py
"""
Purpose: Carry forward market price prior to suspension during the suspension period; implement corporate events without PAF on effective date during technical suspension; implement all other events (with PAF) when security resumes trading.
Datapoints: security_id, closing_price, is_suspended, is_technical_suspension
Thresholds: none
Source: meth-pipeline/MSCI_Corporate_Events_Methodology_20260210/2026-04-23T13-31-41Z/input/markdown.md section "5.1 Suspensions" near line 5057.
See also: methbooks/methodologies/MSCI/corporate_events.py (corporate events apply pipeline).
"""
from __future__ import annotations

import polars as pl


def suspension_price_carryforward(df: pl.DataFrame) -> pl.DataFrame:
    required = ["security_id", "closing_price", "is_suspended", "is_technical_suspension"]
    missing = [c for c in required if c not in df.columns]
    assert not missing, f"missing required columns: {missing}"

    out = df

    cp_min = float(out["closing_price"].min())
    assert cp_min > 0, (
        f"suspended security price = last price prior to suspension during suspension period; "
        f"closing_price must be > 0, actual min: {cp_min}"
    )

    # During suspension, closing_price must equal last_price_prior_to_suspension.
    if "last_price_prior_to_suspension" in out.columns:
        suspended_rows = out.filter(pl.col("is_suspended"))
        import math
        for row in suspended_rows.select(
            ["security_id", "closing_price", "last_price_prior_to_suspension"]
        ).to_dicts():
            assert math.isclose(
                row["closing_price"], row["last_price_prior_to_suspension"], rel_tol=1e-9
            ), (
                f"suspended security price must equal last price prior to suspension; "
                f"security_id: {row['security_id']}, "
                f"closing_price: {row['closing_price']}, "
                f"last_price_prior_to_suspension: {row['last_price_prior_to_suspension']}"
            )

    return out
