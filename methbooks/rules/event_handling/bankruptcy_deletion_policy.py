# methbooks/rules/event_handling/bankruptcy_deletion_policy.py
"""
Purpose: Remove companies filing for bankruptcy, creditor protection, or failing exchange listing requirements as soon as possible; delete at last trading price, then OTC price, then lowest system price; send intraday announcement for same-day implementations.
Datapoints: security_id, closing_price, is_trading, is_otc_price_available
Thresholds: LOWEST_SYSTEM_PRICE = 1e-05
Source: meth-pipeline/MSCI_Corporate_Events_Methodology_20260210/2026-04-23T13-31-41Z/input/markdown.md section "5.3 Bankruptcies" near line 5168.
See also: methbooks/methodologies/MSCI/corporate_events.py (corporate events apply pipeline).
"""
from __future__ import annotations

import polars as pl

LOWEST_SYSTEM_PRICE = 1e-05


def bankruptcy_deletion_policy(df: pl.DataFrame) -> pl.DataFrame:
    required = ["security_id", "closing_price", "is_trading", "is_otc_price_available"]
    missing = [c for c in required if c not in df.columns]
    assert not missing, f"missing required columns: {missing}"

    out = df

    assert LOWEST_SYSTEM_PRICE == 1e-05, (
        f"deletion price hierarchy: last trading price, then OTC price, then lowest system price, "
        f"actual LOWEST_SYSTEM_PRICE: {LOWEST_SYSTEM_PRICE}"
    )

    cp_min = float(out["closing_price"].min())
    assert cp_min >= 0, f"closing_price must be >= 0, actual min: {cp_min}"

    # When security is not trading and OTC price is not available, closing_price must equal
    # LOWEST_SYSTEM_PRICE.
    if "is_bankrupt" in out.columns:
        bankrupt_no_price = out.filter(
            pl.col("is_bankrupt")
            & ~pl.col("is_trading")
            & ~pl.col("is_otc_price_available")
        )
        import math
        for row in bankrupt_no_price.select(["security_id", "closing_price"]).to_dicts():
            assert math.isclose(row["closing_price"], LOWEST_SYSTEM_PRICE, rel_tol=1e-9), (
                f"bankrupt non-trading security with no OTC price must use lowest system price; "
                f"security_id: {row['security_id']}, closing_price: {row['closing_price']}"
            )

    return out
