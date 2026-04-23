# methbooks/rules/event_handling/ma_target_deletion_price.py
"""
Purpose: Delete target securities at closing market prices; when target has ceased trading prior to deletion, use a price reflecting deal terms based on acquirer's market price.
Datapoints: security_id, closing_price, is_trading
Thresholds: none
Source: meth-pipeline/MSCI_Corporate_Events_Methodology_20260210/2026-04-23T13-31-41Z/input/markdown.md section "2.1.2 Implementation price" near line 404.
See also: methbooks/methodologies/MSCI/corporate_events.py (corporate events apply pipeline).
"""
from __future__ import annotations

import polars as pl


def ma_target_deletion_price(df: pl.DataFrame) -> pl.DataFrame:
    required = ["security_id", "closing_price", "is_trading"]
    missing = [c for c in required if c not in df.columns]
    assert not missing, f"missing required columns: {missing}"

    out = df

    assert float(out["closing_price"].min()) >= 0, (
        f"closing_price must be >= 0 for all securities, "
        f"actual min: {float(out['closing_price'].min())}"
    )

    # When target has ceased trading, deletion price must be derived from
    # acquirer market price times exchange ratio; validate via proxy column.
    if "deletion_price_source" in out.columns:
        not_trading = out.filter(~pl.col("is_trading"))
        invalid = not_trading.filter(
            pl.col("deletion_price_source") == "market"
        )
        assert invalid.is_empty(), (
            f"non-trading targets must use acquirer-derived price, not market price; "
            f"offending security_ids: {invalid['security_id'].to_list()}"
        )

    if "weight" in out.columns:
        total_weight = float(out["weight"].sum())
        assert total_weight >= 0, (
            f"deleted security weight must be >= 0 on effective date, actual: {total_weight}"
        )

    return out
