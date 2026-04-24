"""
Purpose: Use prices provided up to the point of a market outage for that day's equity index calculation, rather than carrying forward the prior day's close.
Datapoints: intraday_price, market_outage_timestamp.
Thresholds: none.
Source: methbooks/data/markdown/MSCI_Index_Policies.md section "Index Calculation and Discretion" near line 535.
See also: methbooks/rules/event_handling/market_closure_price_carryforward.py (closure-level carryforward).
"""
from __future__ import annotations
import polars as pl

def market_outage_partial_day_prices(df: pl.DataFrame) -> pl.DataFrame:
    required = ["intraday_price", "market_outage_timestamp"]
    missing = [c for c in required if c not in df.columns]
    assert not missing, f"missing required columns: {missing}"

    out = df
    # On outage days (market_outage_timestamp is not null), intraday_price must not be null.
    outage_rows = out.filter(pl.col("market_outage_timestamp").is_not_null())
    if outage_rows.height > 0:
        null_price_count = outage_rows["intraday_price"].null_count()
        assert null_price_count == 0, (
            f"on outage days, intraday_price must be populated (last price before outage); "
            f"null_price_count={null_price_count}"
        )
    return out
