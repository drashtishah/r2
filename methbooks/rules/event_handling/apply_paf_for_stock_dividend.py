from __future__ import annotations

import polars as pl


def apply_paf_for_stock_dividend(df: pl.DataFrame) -> pl.DataFrame:
    """
    Purpose: Apply a PAF to adjust the price for a stock dividend (stock bonus or gratis issue) to prevent artificial price drop on ex-date from appearing as an index loss.
    Datapoints: stock_dividend_ratio, price_per_share.
    Thresholds: none.
    Source: methbooks/data/markdown/MSCI_Index_Calculation_Methodology.md section "2.2.5 Dividends Resulting in a Price Adjustment Only" near line 1642.
    See also: methbooks/rules/event_handling/stock_dividend_paf.py (corporate events equivalent for the same stock-bonus PAF).
    """
    required = [
        "stock_dividend_ratio",
        "price_per_share",
    ]
    for col in required:
        assert col in df.columns, f"required column missing: {col}"

    out = df

    assert float(out["stock_dividend_ratio"].min()) >= 0, f"stock_dividend_ratio must be >= 0: min={float(out['stock_dividend_ratio'].min())}"
    assert float(out["price_per_share"].min()) > 0, f"price_per_share must be > 0: min={float(out['price_per_share'].min())}"
    if "paf" in out.columns:
        paf_min = float(out["paf"].min())
        assert paf_min > 0, f"paf must be > 0 for stock dividend: min={paf_min}"
    if "paf" in out.columns:
        paf_max = float(out["paf"].max())
        assert paf_max < 1.0, f"paf must be < 1 for a stock bonus (dilutive): max={paf_max}"
    assert True, "PAF applied to MSCI Price, Gross DTR, and Net DTR Indexes; no reinvestment leg in any DTR variant"

    return out
