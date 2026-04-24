"""
Purpose: Fail the liquidity screening for any non-constituent security whose stock
price exceeds USD 10,000; current constituents remain eligible at prices above
this threshold.
Datapoints: stock_price_usd, is_existing_imi_constituent.
Thresholds: MAX_STOCK_PRICE_USD_NON_CONSTITUENT=10000.
Source: meth-pipeline/MSCI_Global_Investable_Market_Indexes_Methodology_20260331/2026-04-24T07-46-35Z/input/markdown.md section "2.2.5 DM and EM Minimum Liquidity Requirement" near line 1122.
See also: methbooks/rules/eligibility/apply_dm_em_minimum_liquidity_requirement.py (primary liquidity screen).
"""
from __future__ import annotations

import polars as pl

MAX_STOCK_PRICE_USD_NON_CONSTITUENT = 10_000.0


def apply_non_constituent_high_price_liquidity_screen(df: pl.DataFrame) -> pl.DataFrame:
    assert "stock_price_usd" in df.columns, (
        f"stock_price_usd column missing: {df.columns}"
    )
    out = df.filter(
        pl.col("is_existing_imi_constituent")
        | (pl.col("stock_price_usd") <= MAX_STOCK_PRICE_USD_NON_CONSTITUENT)
    )
    bad = out.filter(
        (~pl.col("is_existing_imi_constituent"))
        & (pl.col("stock_price_usd") > MAX_STOCK_PRICE_USD_NON_CONSTITUENT)
    )
    assert bad.height == 0, (
        f"non-constituent with stock_price_usd > {MAX_STOCK_PRICE_USD_NON_CONSTITUENT}"
        f" survived: {bad.height} rows;"
        f" max stock_price_usd={bad['stock_price_usd'].max()}"
    )
    return out
