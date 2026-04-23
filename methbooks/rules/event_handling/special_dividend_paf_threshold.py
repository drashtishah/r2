# methbooks/rules/event_handling/special_dividend_paf_threshold.py
"""
Purpose: Apply PAF = [(P(t) + Special Div) / P(t)] on ex-date if special cash dividend is >= 5% of market price on announcement date in confirmed status; reinvest without PAF if below 5%.
Datapoints: security_id, closing_price, special_dividend_amount
Thresholds: PAF_TRIGGER_PCT_OF_MARKET_PRICE = 5
Source: meth-pipeline/MSCI_Corporate_Events_Methodology_20260210/2026-04-23T13-31-41Z/input/markdown.md section "3.4 Special Cash Dividends" near line 4026.
See also: methbooks/methodologies/MSCI/corporate_events.py (corporate events apply pipeline).
"""
from __future__ import annotations

import polars as pl

PAF_TRIGGER_PCT_OF_MARKET_PRICE = 5


def special_dividend_paf_threshold(df: pl.DataFrame) -> pl.DataFrame:
    required = ["security_id", "closing_price", "special_dividend_amount"]
    missing = [c for c in required if c not in df.columns]
    assert not missing, f"missing required columns: {missing}"

    out = df

    assert PAF_TRIGGER_PCT_OF_MARKET_PRICE == 5, (
        f"PAF trigger threshold must be 5% of market price at confirmed status, "
        f"actual: {PAF_TRIGGER_PCT_OF_MARKET_PRICE}"
    )

    cp_min = float(out["closing_price"].min())
    assert cp_min > 0, f"closing_price must be > 0, actual min: {cp_min}"

    sd_min = float(out["special_dividend_amount"].min())
    assert sd_min >= 0, f"special_dividend_amount must be >= 0, actual min: {sd_min}"

    if "paf" in out.columns and "market_price_at_confirmed" in out.columns:
        triggered = out.filter(
            (pl.col("special_dividend_amount") / pl.col("market_price_at_confirmed")) * 100
            >= PAF_TRIGGER_PCT_OF_MARKET_PRICE
        )
        missing_paf = triggered.filter(pl.col("paf").is_null())
        assert missing_paf.is_empty(), (
            f"PAF must be applied when special_dividend_amount / market_price_at_confirmed "
            f">= {PAF_TRIGGER_PCT_OF_MARKET_PRICE / 100}; "
            f"offending security_ids: {missing_paf['security_id'].to_list()}"
        )

    return out
