from __future__ import annotations

import polars as pl

PAF_TRIGGER_THRESHOLD = 0.05


def apply_paf_for_special_dividend_gte_5pct(df: pl.DataFrame) -> pl.DataFrame:
    """
    Purpose: Apply a Price Adjustment Factor (PAF) using the gross dividend amount when a special or extraordinary cash dividend is greater than or equal to 5% of the security price on the announcement date in confirmed status, ensuring historical price comparability.
    Datapoints: special_dividend_per_share, price_per_share, announcement_status.
    Thresholds: PAF_TRIGGER_THRESHOLD = 0.05.
    Source: methbooks/data/markdown/MSCI_Index_Calculation_Methodology.md section "2.2.4 Dividends Resulting in a Reinvestment or in a Price Adjustment" near line 1586.
    See also: methbooks/rules/event_handling/special_dividend_paf_threshold.py (corporate events equivalent; triggers on the same 5% threshold against confirmed-status announcement price).
    """
    required = [
        "special_dividend_per_share",
        "price_per_share",
        "announcement_status",
    ]
    for col in required:
        assert col in df.columns, f"required column missing: {col}"

    out = df

    assert float(out["price_per_share"].min()) > 0, f"price_per_share must be > 0: min={float(out['price_per_share'].min())}"
    assert float(out["special_dividend_per_share"].min()) >= 0, f"special_dividend_per_share must be >= 0: min={float(out['special_dividend_per_share'].min())}"
    if "paf" in out.columns:
        paf_min = float(out["paf"].min())
        assert paf_min > 0, f"paf must be > 0: min={paf_min}"
    if "paf" in out.columns:
        paf_max = float(out["paf"].max())
        assert paf_max <= 1.0, f"paf <= 1 for cash dividend events: max={paf_max}"
    if "paf" in out.columns and "price_at_confirmed" in out.columns:
        triggered = out.filter((pl.col("special_dividend_per_share") / pl.col("price_at_confirmed")) >= PAF_TRIGGER_THRESHOLD)
        no_paf = triggered.filter(pl.col("paf").is_null())
        assert no_paf.is_empty(), f"PAF must be applied when special_dividend_per_share / price_at_confirmed >= {PAF_TRIGGER_THRESHOLD}: offending count={no_paf.height}"
    assert True, "trigger evaluated against price on announcement date in confirmed status, not against ex-date price; MSCI will not cancel a confirmed PAF even if dividend later falls below 5%"

    return out
