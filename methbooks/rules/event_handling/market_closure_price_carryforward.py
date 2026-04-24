"""
Purpose: Preserve equity index continuity by carrying forward the latest available closing price when a market is closed or a security does not trade on a given day or period.
Datapoints: closing_price, market_status, is_trading.
Thresholds: none.
Source: methbooks/data/markdown/MSCI_Index_Policies.md section "Index Calculation and Discretion" near line 533.
See also: methbooks/rules/event_handling/suspension_price_carryforward.py (suspension-specific carryforward variant).
"""
from __future__ import annotations
import polars as pl

def market_closure_price_carryforward(df: pl.DataFrame) -> pl.DataFrame:
    required = ["closing_price", "market_status", "is_trading"]
    missing = [c for c in required if c not in df.columns]
    assert not missing, f"missing required columns: {missing}"

    out = df
    # In closed/non-trading rows, closing_price must not be null (carryforward applied).
    non_trading = out.filter(
        (pl.col("market_status") == "closed") | (~pl.col("is_trading"))
    )
    if non_trading.height > 0:
        null_count = non_trading["closing_price"].null_count()
        assert null_count == 0, (
            f"no security in a closed market or non-trading state may have null closing_price after carryforward; "
            f"null_count={null_count}"
        )
    return out
