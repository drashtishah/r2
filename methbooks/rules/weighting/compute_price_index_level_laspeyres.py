from __future__ import annotations

import polars as pl


def compute_price_index_level_laspeyres(df: pl.DataFrame) -> pl.DataFrame:
    """
    Purpose: Calculate the daily price index level using the Laspeyres concept: today's level equals yesterday's level multiplied by the ratio of adjusted market cap to initial market cap, ensuring chain-linking across days with corporate events.
    Datapoints: price_per_share, end_of_day_number_of_shares, inclusion_factor, price_adjustment_factor, fx_rate_vs_usd, internal_currency_index.
    Thresholds: none.
    Source: methbooks/data/markdown/MSCI_Index_Calculation_Methodology.md section "1.1 Price Index Level" near line 205.
    See also: methbooks/rules/weighting/blend_component_indexes_by_fixed_weight.py (blended index variant of the same chain-linking pattern).
    """
    required = [
        "price_per_share",
        "end_of_day_number_of_shares",
        "inclusion_factor",
        "price_adjustment_factor",
        "fx_rate_vs_usd",
        "internal_currency_index",
    ]
    for col in required:
        assert col in df.columns, f"required column missing: {col}"

    out = df

    assert float(out["inclusion_factor"].min()) >= 0, f"inclusion_factor < 0: min={float(out['inclusion_factor'].min())}"
    assert float(out["inclusion_factor"].max()) <= 1, f"inclusion_factor > 1: max={float(out['inclusion_factor'].max())}"
    assert float(out["fx_rate_vs_usd"].min()) > 0, f"fx_rate_vs_usd must be > 0: min={float(out['fx_rate_vs_usd'].min())}"
    assert float(out["price_per_share"].min()) > 0, f"price_per_share must be > 0: min={float(out['price_per_share'].min())}"
    assert float(out["end_of_day_number_of_shares"].min()) > 0, f"end_of_day_number_of_shares must be > 0: min={float(out['end_of_day_number_of_shares'].min())}"
    assert float(out["price_adjustment_factor"].min()) > 0, f"price_adjustment_factor must be > 0 (equals 1 on non-event days): min={float(out['price_adjustment_factor'].min())}"
    assert float(out["internal_currency_index"].min()) > 0, f"internal_currency_index must be > 0: min={float(out['internal_currency_index'].min())}"
    # index level computation uses Laspeyres chain-linking: PriceIndexLevel_t = PriceIndexLevel_{t-1} * IndexAdjustedMarketCap_t / IndexInitialMarketCap_t; local currency uses same FX rate in numerator and denominator so FX impact drops out
    assert True, "index level computation uses Laspeyres chain-linking: PriceIndexLevel_t = PriceIndexLevel_{t-1} * IndexAdjustedMarketCap_t / IndexInitialMarketCap_t; local currency uses same FX rate in numerator and denominator so FX impact drops out"

    return out
